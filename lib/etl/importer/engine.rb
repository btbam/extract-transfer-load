module ETL
  module Importer
    class Engine
      extend Forwardable

      # delegate to @options all calls matching keys in DEFAULT_OPTIONS, plus the dispatcher
      def_delegators :@options, *DEFAULT_OPTIONS.keys
      def_delegators :@dispatcher, :invoke_callback

      attr_accessor :logger, :validation_logger, :benchmarks, :validation_errors, :records_created, :records_updated
      attr_reader :options, :thread_pool, :data_scrubber, :importer_run, :transformer

      def initialize(**opts)
        @options = OpenStruct.new(DEFAULT_OPTIONS.merge(opts))

        # If a logger is provided, use it. If deliberately SET to nil/false, print to null. Default to STDOUT.
        @logger = options.logger || (opts.key?(:logger) && !options.logger) && Logger.new(nil) || Logger.new(STDOUT)
        @logger.formatter = proc do |severity, datetime, progname, msg|
          thread_id = Thread.current[:id]
          thread_stamp = thread_id ? "(#{thread_id})" : "(M)"
          "[#{datetime.strftime("%D %T.%6N")}] #{severity} #{thread_stamp} : #{msg}\n"
        end

        # if a validation logger is provided, use it, if nil/false print to null. Default to log/importer_validation_errors.log
        @validation_logger = options.validation_logger || (opts.key?(:validation_logger) && !options.validation_logger) && Logger.new(nil) || Logger.new(File.join(Rails.root, 'log', 'importer_validation_errors.log'))

        @benchmarks = Hash.new{|h,k| h[k] = []}
        @validation_errors = {}
        @records_created = 0
        @records_updated = 0

        @dispatcher = Dispatcher.new(context, callbacks)

        at_exit { thread_pool.shutdown }
      end

      # this runs once at the very beginning of the import process.
      def setup
        # suppress ActiveRecord logging
        ActiveRecord::Base.logger.level = Logger::WARN

        @importer_run = ImporterRun.new
        @transformer = Transformer.new(context: context, transformations: transformations, options: transformation_options)
        @thread_pool = ETL::ThreadPool.new(options.pool)
        @data_scrubber = ETL::DataScrubber.new(options.destination_model)
        invoke_callback(:before_run)
      end

      def columns
        @columns ||= transformations.keys
      end

      # this method actually performs the import
      # it iterates over every row in the source db, and creates an attributes hash to be inserterd into destination.
      # if a block is provided, the attributes for the new object are yielded as a hash as the first argument
      # to the block, and the source record object is optionally yielded as the second. The user can then modify the
      # attrs hash and implicitly return it (or call 'next attrs') to save that object. If nil/false is returned
      # from the block, the record is skipped.
      def run
        start_time = Time.now

        setup

        importer_run.update_attributes( started_at: start_time,
                                        source_model: source_model.name,
                                        destination_model: destination_model.name,
                                        importer_version: VERSION )

        # get a total count of records to process, bail out if none are found
        count = base_query(false).count

        logger.info ""
        if count == 0
          logger.info "no #{source_model.name.pluralize} to import, exiting"
          return
        end

        logger.info "Starting import from #{source_model.table_name} into #{destination_model.name}..."

        # step through the records in batches
        (0..count).step(batch_size) do |offset|
          thread_pool.schedule do
            with_connections do
              batch_elapsed_time = Benchmark.realtime do
                logger.info "Importing from #{source_model.table_name} into #{destination_model.name} (#{offset / batch_size + 1}/#{count / batch_size + 1})"

                # wipe the slate from the last batch
                prepare_for_new_batch

                benchmarks[:source_db] << Benchmark.realtime do
                  # grab this batch of records from source
                  fetch_records(offset)
                end

                # bail if there aren't any
                next if records.empty?

                logger.info "  #{records.count} source records fetched in #{benchmarks[:source_db].last}s"
                if source_order_by
                  logger.info "  #{source_order_by}: from #{records.first.read_attribute(source_order_by)} to #{records.last.read_attribute(source_order_by)}"
                end

                # process this batch of records
                process_batch(records)

                logger.info "  #{records.count} records processed in #{benchmarks[:processing].last}s"

                insert_and_update_batch
              end
              logger.info "  batch processed in #{batch_elapsed_time}s"
            end
          end
        end
        thread_pool.shutdown
        
        print_validation_errors

        logger.info "-------------------------------------------------"
        logger.info "Processing:      #{benchmarks[:processing].sum}s total, #{benchmarks[:processing].sum / count}s per record"
        logger.info "source Database: #{benchmarks[:source_db].sum}s total, #{benchmarks[:source_db].sum / count}s per record"
        logger.info "dest Database:   #{benchmarks[:destination_db].sum}s total, #{benchmarks[:destination_db].sum / count}s per record"
        logger.info "Total:           #{Time.now - start_time}s elapsed"
        importer_run.update_attributes( completed_at: Time.now )
      rescue Exception => e
        importer_run.update_attributes( error_trace: "#{e.class} - #{e.message}\n#{e.backtrace.join("\n")}" )
        raise e
      ensure
        importer_run.update_attributes( records_created: records_created,
                                        records_updated: records_updated,
                                        duration: ((Time.now - start_time) * 1000).round,
                                        validation_errors: validation_errors )
      end

      # wrap the block in connection checkout/checkin blocks for each database
      def with_connections
        source_model.connection_pool.with_connection do
          destination_model.connection_pool.with_connection do
            yield
          end
        end
      end

      # the relation through which the importer steps and from which it imports records
      def base_query(select_columns = true)
        if source_query
          if select_columns && source_select_columns
            source_query.call.select(*source_select_columns).where(source_conditions)
          else
            source_query.call.where(source_conditions)
          end
        else
          query = source_model.where(source_conditions).order(source_order_by => :asc)
          query = query.select(*source_select_columns) if select_columns && source_select_columns
          !force_full_update && highest_known_destination_order_by ? query.where("#{source_order_by} >= ?", highest_known_destination_order_by) : query
        end
      end

      # returns true if this record already exists in destination and needs to be updated, else false
      def update?(record, attrs)
        return false unless update_on
        existing_key = attrs[update_on.to_sym]
        existing_ids.key?(existing_key)
      end

      # returns a hash of update column value to primary keys of destination model records
      def existing_ids
        return Thread.current[:existing_ids] if Thread.current[:existing_ids]

        ids_in = [].tap do |ids|
          transformer.map_column(update_on, records).each_slice(999) do |slice|
            ids << destination_model.arel_table[update_on].in(slice).to_sql
          end
        end  
        
        Thread.current[:existing_ids] = Hash[destination_model.where(ids_in.join(" OR ")).pluck(update_on, :id)]
      end

      # returns the newest source_order_by in the destination table, or nil if no records exist
      def highest_known_destination_order_by
        return false unless source_order_by
        if defined? @highest_known_destination_order_by 
          @highest_known_destination_order_by
        else
          @highest_known_destination_order_by = destination_model.where(destination_order_conditions).maximum(destination_order_by)
        end
      end

      def thread_id
        Thread.current[:id] || (raise RuntimeError, "thread id not set on the current thread!")
      end  

      # clear the decks for a new batch
      def prepare_for_new_batch
        invoke_callback(:before_each_batch)

        new_rows.clear
        update_attrs.clear
        records.clear
        Thread.current[:existing_ids] = nil
      end

      def new_rows
        Thread.current[:new_rows] ||= []
      end

      def update_attrs
        Thread.current[:update_attrs] ||= []
      end

      def records
        Thread.current[:records] ||= []
      end

      def fetch_records(offset)
        Thread.current[:records] = base_query.offset(offset).limit(batch_size).to_a
      end

      # process this batch of records
      def process_batch(records)
        benchmarks[:processing] << Benchmark.realtime do
          records.each do |record|
            next if skip_before_transform?(record)
            invoke_callback(:before_each, record)
            attrs = transformer.run(record)
            next unless attrs # skip if transformation failed
            invoke_callback(:each_before_save, attrs, record)
            next if invoke_callback(:reject_after_transform_if, attrs)
            invoke_callback(:after_each, record, attrs)
            attrs = data_scrubber.scrub!(attrs)
            
            if update?(record, attrs)
              update_attrs << attrs
            elsif create_new_records
              new_rows << attrs.values
            end
          end
        end
      end

      # returns true if a record should be skipped before it is transformed
      def skip_before_transform?(record)
        invoke_callback(:reject!, record)
      end

      # keep a running tally of validation errors, which field had which error messages how many times
      def record_validation_errors(failed_instances)
        failed_instances = [*failed_instances].flatten
        logger.info "0 validation errors!" if failed_instances.empty?
        validation_errors[:total] ||= 0
        validation_errors[:total] += failed_instances.count
        errors = failed_instances.map { |fi| fi.errors.messages }

        if validation_logger
          failed_instances.each do |fi|
            validation_logger.info fi.errors.inspect
          end
        end

        errors.each do |error|
          error.each_pair do |field, messages|
            validation_errors[field] ||= {}
            messages.each do |message|
              validation_errors[field][message] ||= 0
              validation_errors[field][message] += 1
            end
          end
        end
      end

      # print a report of validation errors to logger
      def print_validation_errors
        return if !validate || validation_errors.empty? || !create_new_records
        logger.info "-------------------------------------------------"
        logger.info "#{validation_errors[:total]} validation errors:"
        validation_errors.each_pair do |field, errors|
          next if field == :total
          logger.info "  #{field}:"
          errors.each_pair do |message, count|
            logger.info "    #{message} (#{count})"
          end
        end
      end

      def insert_and_update_batch
        # if @use_db is truthy, batch insert rows into destination.
        # runs validations and records errors if @validate is truthy.
        if use_db
          results = nil
          benchmarks[:destination_db] << Benchmark.realtime do
            if create_new_records
              benchmarks[:database_insert] << Benchmark.realtime do
                results = destination_model.import(columns, new_rows, :validate => validate)
              end
              if validate && !results.failed_instances.empty?
                record_validation_errors(results.failed_instances)
              end
            end

            if update_on
              benchmarks[:database_update] << Benchmark.realtime do
                update_hash = {}
                update_attrs.each do |attrs|
                  destination_id = existing_ids[attrs[update_on]]
                  update_hash[destination_id] = attrs
                end
                res = destination_model.update(update_hash.keys, update_hash.values)
              end
            end

          end

          if create_new_records
            batch_records_created = new_rows.count - results.failed_instances.count
            self.records_created += batch_records_created
            logger.info "  #{batch_records_created}/#{new_rows.count} new rows saved in #{benchmarks[:destination_db].last}s (#{benchmarks[:database_insert].last}s db)"
          end
          
          if update_on
            batch_records_updated = update_attrs.count
            self.records_updated += batch_records_updated
            logger.info "  #{batch_records_updated} existing rows updated in #{benchmarks[:database_update].last}s"
          end
        end
      end
    end
  end
end
