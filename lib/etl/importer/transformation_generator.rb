# constructs a proc that, when passed a record, returns the value for
# the destination record for the specified column

module ETL
  module Importer
    class TransformationGenerator
      attr_accessor :column, :instruction, :context

      def initialize(column: nil, instruction: nil, context: nil)
        @column = column
        @instruction = instruction
        @context = context
      end

      # returns a proc which can be passed an object from the source
      # and yields a value which can be assigned to the target
      def generate
        if static_reassignment?
          method(:assign_static_column)
        elsif instruction_callable?
          instruction
        elsif instruction_empty?
          if context_method_exists?
            method(:assign_context_method)
          elsif target_has_attribute?(column)
            method(:assign_target_attribute)
          else
            method(:assign_null_attribute)
          end
        else
          raise "unrecognized arguments for transformalizer: #{self.inspect}"
        end
      end

      private

      # true if the target model has the specified attribute
      def target_has_attribute?(column)
        context.engine.source_model.columns.detect { |c| c.name == column.to_s }
      end

      # true if the transformation is a verbatim copy from source to target
      def static_reassignment?
        [Symbol, String].include?(instruction.class)
      end
      
      # true if a transformation is already specified in the form of a proc
      def instruction_callable?
        instruction.respond_to?(:call)
      end

      # true if no transformation is explicitly specified
      def instruction_empty?
        instruction.nil?
      end

      # true if a method matching the column name exists on the context
      def context_method_exists?
        context.respond_to?(column)
      end

      # The following assignment methods exist to be unbound and passed
      # to the transformer and called for each record passed to the transformer

      def assign_static_column(object)
        object.read_attribute(instruction)
      end

      def assign_context_method(object)
        context.send(column, object)
      end

      def assign_target_attribute(object)
        object.read_attribute(column)
      end

      def assign_null_attribute(*)
        nil
      end

    end
  end
end
