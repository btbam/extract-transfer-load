require 'forwardable'

module ETL
  class DataScrubber
    extend Forwardable
    def_delegator 'self.class', :scrub_text

    attr_accessor :model

    class << self
      unless defined? TEXT_GSUBS
        TEXT_GSUBS = [
          [/\d\d\d-\d\d-\d\d\d\d/, '###-##-####'],
          [/(^|\D)(\d{9})(?!\d)/, '\1#########']
        ]
      end

      # allows calling without instantiating
      def scrub_text(text)
        TEXT_GSUBS.inject(text) { |memo, sub| memo = memo.gsub(*sub) }
      end
    end    

    def initialize(model)
      @model = model
    end

    def scrub!(attrs)
      attrs.each_pair do |name, value|
        coltype = column_type(name)
        fail "Column #{name.inspect} not found" unless coltype
        case column_type(name)
        when :text
          attrs[name] &&= scrub_text(value)
        end
      end
      attrs
    end

    private

    def column_type(name)
      column_map[name.to_sym]
    end

    def column_map
      @column_map ||= Hash[model.columns.map { |c| [c.name.to_sym, c.type.to_sym] }]
    end
  end
end
