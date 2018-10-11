require 'forwardable'

# scrubs personally identifiable information from records in the db
module ETL
  class DatabaseScrubber
    extend Forwardable

    # forward some messages to @options
    def_delegators :@options, :batch_size, :columns, :model

    attr_accessor :options, :logger
    attr_reader :thread_pool, :data_scrubber

    def initialize(**opts)
      @options = OpenStruct.new(option_defaults.merge(opts))

      # If a logger is provided, use it. If deliberately SET to nil/false, print to null. Default to STDOUT.
      @logger = options.logger || (opts.key?(:logger) && !options.logger) && Logger.new(nil) || Logger.new(STDOUT)

      @thread_pool = ThreadPool.new(@options.pool)
      @data_scrubber = DataScrubber.new(model)

      at_exit { @thread_pool.shutdown }

      check_options!
    end

    # default values for options, with explanations
    def option_defaults
      {
        pool: 4,
        batch_size: 1000,
        columns: [],
        model: nil
      }
    end

    def check_options!
      fail 'you must specify columns' if columns.empty?
      fail 'you must specify a model' unless model
    end

    def run
      # suppress ActiveRecord logging
      ActiveRecord::Base.logger.level = Logger::WARN

      count = model.count

      # step through the records in batches
      (0..count).step(batch_size) do |offset|
        thread_pool.schedule do
          ActiveRecord::Base.connection_pool.with_connection do
            logger.info "worker #{Thread.current[:id]}: processing (#{offset / batch_size + 1}/#{count / batch_size + 1})"
            notes = model.order(id: :asc).offset(offset).limit(batch_size).select(*(columns.map(&:to_sym) + [:id]).uniq)
            process_batch(notes)
          end
        end
      end

      thread_pool.shutdown
    end

    def process_batch(notes)
      update_hash = {}

      notes.each do |note|
        # get the attributes from this note
        attrs = note.attributes

        # scrub a *copy* of the :attrs hash into :scrubbed_attrs
        scrubbed_attrs = data_scrubber.scrub!(attrs.dup)

        # don't update records that haven't been changed at all
        unless scrubbed_attrs == attrs
          # the use of the guarded assignment operator here is a little bit of trickery.
          # it forces the left side of the assignment to be evaluated first, ensuring that
          # we delete the "id" key from the hash and assign the rest of the attrs hash into
          # that location in update_hash
          update_hash[scrubbed_attrs.delete("id")] ||= scrubbed_attrs
        end
      end

      model.update(update_hash.keys, update_hash.values)
    end
  end
end