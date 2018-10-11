module ETL
  module Importer
    unless defined? DEFAULT_OPTIONS
      # default values for options, with explanations
      DEFAULT_OPTIONS = {
        # a hash of column names of the destination model, mapped to
        # - symbol, string - will call the method on the source record
        # - proc, lambda - will be called with the record as the argument
        # - nil - a nil in the destination attrs
        transformations: {},
        # the model corresponding to the source table to be imported
        source_model: nil,
        # the model corresponding to the destination table to be populated
        destination_model: nil,
        # the name of a column on the destination model which should be used as a key
        # to update existing records, rather than inserting new ones
        update_on: nil,
        # a column to use for ordering the source table such that stepping over the rows in batches
        # allows a consistent ordering. the order in which records will be imported.
        source_order_by: nil,
        # a destination column which contains values transferred from the destination_order_by column,
        # in order to determine the highest known value for incremental updating
        destination_order_by: nil,
        # a hash of additional conditions to be used to find the newest known id for the destination_order_by
        # option, above, in order to narrow the ordering on a per-source-table basis
        # this is only used in the highest_known_destination_order_by method
        destination_order_conditions: {},
        # if true, import the entire table, ignoring source ordering and the highest known id
        force_full_update: false,
        # if false, only update existing rows, do not create new records.
        create_new_records: true,
        # the size of batches to be used when stepping trough the source table
        batch_size: 10000,
        # a flag which determines whether or not to actually insert rows into destination
        use_db: true,
        # a flag which determines whether validations should be applied upon insert
        validate: true,
        # an optional lambda/proc, which, if provided, serves as the base query to
        # source that the importer steps through in chunks using offsets. If this is used,
        # the timestamps are ignored.
        source_query: nil,
        # additional conditions that are chained on to the source base query. keys are methods
        # that exist on ActiveModel::Relation (or a model class), and values are the arguments
        # to be passed to those methods. for example { rewhere: { column_1: nil }} would result
        # in .rewhere(column_1: nil) being chained onto the source relation over which the importer
        # iterates
        source_conditions: {},
        # the number of threads to import with
        pool: 4,
        # if specified, the query on the source is scoped to just the listed columns
        source_select_columns: nil,
        # extra options for each column
        transformation_options: Hash.new { |h,k| h[k] = {}},
        # if a context is provided, this engine is an instantiated subclass of ETL::Importer::Base
        # The context is that instance. If the engine is instantiated directly, this defaults to nil
        context: nil,
        # a hash mapping callback names (Symbols) to the body of the callbacks (Procs)
        callbacks: {}
      }
    end
  end
end