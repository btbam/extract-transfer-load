# transforms individual records to produce attributes hashes

module ETL
  module Importer
    class Transformer
      attr_reader :object, :context, :transformations, :options

      def initialize(context: context, transformations: transformations, options: options)
        @context = context
        @options = options
        @transformations = generate_transformations(transformations)
      end

      # returns hash of attributes for destination, or false if validation failed
      def run(object)
        {}.tap do |output|
          transformations.each_pair do |column, transformation|
            value = transformation.call(object)
            return false unless valid?(column, value)
            output[column] = apply_try(column, value)
          end
        end
      end

      def run_column(column, object)
        transformations[column.to_sym].call(object)
      end

      def map_column(column, objects)
        objects.map { |object| run_column(column, object) }
      end

      private

      # convert the options into procs which can be called on each source object
      # this reduces the amount of logic that has to run per record to the bare minimum
      def generate_transformations(transformation_options)
        {}.tap do |transforms|
          transformation_options.each do |column, instruction|
            generator = TransformationGenerator.new(
                            column: column,
                            instruction: instruction,
                            context: context
                          )
            transforms[column] = generator.generate
          end
        end
      end

      # applies any "try" transformations and returns the new value
      def apply_try(column, value)
        return value unless options[column].key?(:try)
        [*options[column][:try]].reduce(value) do |val, meth|
          val.respond_to?(meth) ? val.send(meth) : val
        end
      end

      # returns true if a column's value is valid, else false
      def valid?(column, value)
        if required?(column) && value.blank?
          false
        else
          true
        end
      end

      # returns true if the given column is required
      def required?(column)
        options[column].key?(:required)
      end
    end
  end
end