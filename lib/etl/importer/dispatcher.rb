module ETL
  module Importer
    # stores and invokes callbacks in a specified context
    class Dispatcher
      attr_accessor :callbacks, :context

      def initialize(context, callbacks = {})
        @context = context
        @callbacks = callbacks
      end

      def register_callback(name, &body)
        fail "unknown callback name: #{name}" unless callback_name_valid?(name)
        initialize_callback(name, &body)
      end

      def invoke_callback(name, *args)
        return nil unless callbacks.key?(name)
        fail "context not set" unless context
        context.instance_exec(*args, &callbacks[name])
      end

      private

      def initialize_callback(name, &body)
        fail "unknown callback name: #{name}" unless callback_name_valid?(name)        
        callbacks[name] = body || Proc.new{}
      end

      def callback_name_valid?(name)
        CALLBACKS.include?(name)
      end
    end
  end
end
