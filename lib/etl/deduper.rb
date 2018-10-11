module ETL
  class Deduper
    extend Forwardable

    # forward some messages to @options
    def_delegators :@options, :partition_size, :batch_size, :pool, :model, :grain, :confirm, :test, :fast_partitioner

    attr_accessor :options, :logger
    attr_reader :thread_pool

    def initialize(**opts)
      @options = OpenStruct.new(OPTION_DEFAULTS.merge(opts))

      # If a logger is provided, use it. If deliberately SET to nil/false, print to null. Default to STDOUT.
      @logger = options.logger || (opts.key?(:logger) && !options.logger) && Logger.new(nil) || Logger.new(STDOUT)

      @thread_pool = ThreadPool.new(pool)

      at_exit { @thread_pool.shutdown }
    end

    OPTION_DEFAULTS = {
        # the number of workers in the thread pool
        pool: 4,

        # the number of unique grain values per partition
        partition_size: 10_000,

        # the number of grain_ids processed per batch
        batch_size: 1000,

        # the model to dedup
        model: Note,

        # the column on which to enforce uniqueness
        grain: :unique_id,

        # if true, confirms that it is not attempting to delete originals
        # this will greatly decrease performance
        confirm: true,

        # if true, it won't actually delete anything
        test: true,

        # 'fast' mode can potentially have memory issues, but will be faster for
        # datasets of intermediate size. it probably isn't much faster for smaller datasets.
        # It will potentially fail on datasets over a certain size, depending on resources
        fast_partitioner: true
    }

    # If invoked within the context of the thread pool scheduler, this will return an integer
    # identifier for the current thread context. If invoked outside the scheduler, it raises an error
    def thread_id
      Thread.current[:id] || nil
    end

    # a string identifying the current thread for logging purposes
    def thread_tag
      (thread_id && "(#{thread_id})") || '(M)'
    end

    # all distinct grain values
    def base_grain_query
      model.select("distinct(#{grain})")
    end

    # all distinct grain values, ordered by grain
    def ordered_base_grain_query
      base_grain_query.order(grain)
    end

    # returns a count of records in base_grain_query
    def base_grain_count
      @base_grain_count ||= base_grain_query.count
    end

    # returns an array of pairs of integers defining the id of the first and last grain value in the partition
    def partitions
      @partitions ||= fast_partitioner ? fast_build_partitions : safe_build_partitions
    end

    # finds partition boundaries in a single query. fast, but possibly dangerous with very large datasets
    def fast_build_partitions
      logger.info "Building Partitions (this may take awhile)..."
      ordered_base_grain_query
        .pluck(grain)
        .each_slice(partition_size)
        .map{ |slice| [slice.first, slice.last] }
    end

    # finds partitions boundaries in a manner that will work regardless of the size of the dataset
    # it can be slow for large datasets. use only when the fast method (above) fails
    def safe_build_partitions
      logger.info "Building Partitions (this may take awhile)..."
      [].tap do |parts|
        0.step(by: partition_size, to: base_grain_count) do |offset|
          logger.info "Building partition for offset: #{offset}"
          parts << [  ordered_base_grain_query.offset(offset).first.read_attribute(grain),
                     ordered_base_grain_query.offset(offset + partition_size - 1).first.try(:read_attribute, grain) || ordered_base_grain_query.last.read_attribute(grain) ]
        end
      end
    end

    # returns all rows with a grain within the specified range, inclusive
    # and ordered by grain
    def rows_in_range(low, high)
      model.where(
        model.arel_table[grain]
        .gteq(low)
        .and(
          model.arel_table[grain]
          .lteq(high)
        )
      ).order(grain)
    end

    # fetches all rows with a grain value in the specified set
    # and returns the ids of all duplicates. It excludes one of each unique
    # value in grain
    def duplicates_in_set(*grain_values)
      # ensure that the input is a flattened array
      ids = [*grain_values].flatten.uniq

      # make a hash of grains to model instances with
      # the corresponding grain
      grains_to_rows = model.where(grain => ids)
              .order(grain)
              .select(:id, grain)
              .to_a
              .group_by(&grain)

      # convert the values to the ids of duplicates,
      # excluding one corresponding to each grain
      grains_to_dups = grains_to_rows.update(grains_to_rows) do |k,v1,v2|
        v2 = v1.map(&:id).sort[1..-1]
      end

      # return just the unique id values, flattened into a single array
      grains_to_dups.values.flatten.uniq
    end

    # deletes all ids in the set provided
    # if in test mde, doesn't actually delete records
    # if in confirm mode, double-checks before deleting (slow)
    def delete_in_set(ids)
      if confirm
        confirm_set(ids)
      end

      # don't do anything unless test mode is off
      unless test
        query = model.where(id: ids)
        logger.info("DELETING #{query.count} duplicate records")
        query.delete_all
      end
    end

    # confirm that the et of ids specified is safe to delete
    def confirm_set(ids)
      query = model.where(id: ids)

      logger.info 'Confirming set...'

      # get a hash of grains => all records with that grain
      grains_in_set = model.where(grain => query.pluck(grain).uniq)
                           .select(:id, grain)
                           .to_a
                           .group_by(&grain)
      
      # for each grain value...
      grains_in_set.each_pair do |gr, rows|

        # collect the ids of records with that grain
        grain_ids = rows.map(&:id)

        # if there is more than one id with that grain,
        # then deletions are necessary
        if grain_ids.count > 1
          # ensure there are the right number of flagged duplicates
          unless grain_ids.count == (grain_ids & ids).count + 1
            fail "ERROR! Wrong number of duplicates detected, bailing out!"
          end
        else
          # if there is not more than 1 row with this grain
          # then this id should not have been flagged, error!
          fail "ERROR! False duplicate detected, bailing out!"
        end
      end      
    end

    # runs the processor
    def run
      # suppress ActiveRecord logging
      ActiveRecord::Base.logger.level = Logger::WARN

      total_elapsed = Benchmark.realtime do

        # iterate over the partitions separately. Wait for each partition to finish before
        # scheduling the next in order to avoid overflowing available RAM with ids
        partitions.each do |part_first, part_last|
          partition_tag = "(#{partitions.index([part_first, part_last]) + 1}/#{partitions.size})"
          partition_total = Benchmark.realtime do
            logger.info "#{thread_tag} - STARTING partition #{partition_tag}"

            # fetch the unique grain values of all rows in this partition
            partition_ids = rows_in_range(part_first, part_last).map(&grain)

            logger.info "#{thread_tag} - FOUND #{partition_ids.count} rows to process"
            
            # get a count of the total number of batches in this partition
            batches_in_partition = partition_ids.length / batch_size + (partition_ids.length % batch_size > 0 ? 1 : 0)
            
            logger.info "#{thread_tag} - SCHEDULING #{batches_in_partition} batches, batch_size #{batch_size}"
            
            # break the partition into batches, queue all the batches in this partition
            partition_ids.each_slice(batch_size).with_index do |grain_values, i|
              thread_pool.schedule do
                batch_elapsed = Benchmark.realtime do
                  ClaimsLabRecord.connection_pool.with_connection do
                    logger.info "#{thread_tag} - STARTING B(#{(i+1)}/#{batches_in_partition}):P#{partition_tag}"

                    # fetch the rows comprising this set and delete dups
                    dups = duplicates_in_set(grain_values)
                    delete_in_set(dups) unless dups.empty?
                  end
                end
                logger.info "#{thread_tag} - FINISHED B(#{(i+1)}/#{batches_in_partition}):P#{partition_tag} in #{batch_elapsed}s"
              end
            end
            logger.info "#{thread_tag} - ALL BATCHES SCHEDULED for partition #{partition_tag}"

            # join and refresh all the threads in the thread pool for the next partition
            thread_pool.reset
          end
          logger.info "#{thread_tag} - FINISHED partition #{partition_tag} in #{partition_total}s"
        end
      end
      logger.info "ALL DONE! - processed #{base_grain_count} unique grain values in #{total_elapsed}s"
      total_batches = (Float(base_grain_count) / batch_size).ceil
      logger.info "#{total_batches} batches processed, #{total_elapsed / total_batches}s per batch"
      true
    rescue Exception => e
      logger.error "------------------------------------------------------------------------------"
      logger.error 'Error - ' + e.class.inspect + ': ' + e.message
      e.backtrace.each{|line| logger.error(line)}
      logger.error "------------------------------------------------------------------------------"
      raise e
    end
  end
end
