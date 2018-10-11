module ETL
  module Importer
    class Base
      class << self
        # if an option matching a key in settings is defined, override the default and
        # assign the new value
        def method_missing(meth, *args, &block)
          if DEFAULT_OPTIONS.key?(meth)
            settings[meth] = args.first || block
          else
            super
          end
        end
        
        # define methods for each possible callback
        CALLBACKS.each do |callback|
          define_method(callback) do |&block|
            raise ArgumentError, "called without a block" unless block
            settings[:callbacks] ||= {}
            settings[:callbacks][callback] = block
          end
        end

        # returns the settings as specified, so far
        def settings
          @settings ||= {}
        end

        # allows the user to pass in a configuration hash
        def options(opts)
          settings.merge!(opts)
        end

        # set a single configuration option key/value pair
        def set(key, value)
          settings[key] = value
        end

        # define what is imported into what
        def transform(from: nil, to: nil)
          fail "you must provide both source (from) and destination (to) models" unless from && to
          settings[:source_model] = from
          settings[:destination_model] = to
        end

        # Maps a specified attribute from source to destination.
        # There are several permissible forms:
        #
        # - If two string/symbol arguments are provided, the first is
        #   interpreted as the source column, and the second as the destination
        #   column. This defines a simple copy operation.
        # - If a string/symbol and a proc are passed, the proc is passed a source
        #   record, and the result is assigned to the column on the destination
        #   specified by the first argument.
        # - If a single string/symbol and a block are passed, the block is passed
        #   a source record, and the result is assigned to the column on the
        #   destination specified by the argument. The block must be defined such
        #   that it accepts a single argument, the source record to be transformed.
        # - If only a string/symbol argument is provided, the following occurs (in
        #   the order of precedence specified):
        #   - If a method is defined on self which has the same name, that method is
        #     called with the source record as the argument, and the result is saved
        #     to the column on the destination corresponding to the argument
        #   - If both source and target have columns by the same name, the value is copied
        #     over from source to destination unmodified.
        #   - If neither of the above are true, the destination record recieves a nil
        #     value for that column.
        def map_attribute(*attr_opts, **hash_args, &block)
          settings[:transformations] ||= {}
          if attr_opts.length == 1
            dest = attr_opts.first
            source = block
          elsif attr_opts.length == 2
            if attr_opts.last.respond_to?(:call)
              dest = attr_opts.first
              source = attr_opts.last
            else
              dest = attr_opts.last
              source = attr_opts.first
            end
          else
            fail "what are you trying to do?"
          end

          settings[:transformations][dest] = source
          register_transformation_option(dest, hash_args)
        end

        # instantiates an importer and runs it
        def run(opts = {})
          importer = self.new(opts)
          importer.run
        end

        private

        def register_transformation_option(column, options)
          settings[:transformation_options] ||= Hash.new { |h,k| h[k] = {} }
          settings[:transformation_options][column.to_sym].merge!(options)
        end
      end

      attr_reader :engine

      # apply block and hash options in that order 
      def initialize(opts = {})
        options = self.class.settings.merge({context: self}.merge(opts))
        @engine = ETL::Importer::Engine.new(options)
      end

      def run
        Rails.application.eager_load!
        engine.run
      end
    end
  end
end
