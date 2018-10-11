module ETL
  module Importer
    unless defined? CALLBACKS
      CALLBACKS = [
        # define a block which is called once before starting the import job
        :before_run,
        # define a block which is called with each record before the transform
        :before_each,
        # define a block which is called with each record after the transform and before the save
        :each_before_save,
        # define a block which is called before each batch
        :before_each_batch,
        # define a block which is called just before the transform. any record
        # that returns a falsy value will not be saved or updated
        :reject!,
        # run after each transformation
        :after_each,
        # define a block which is called just after the transform. any record
        # that returns a falsy value will not be saved or updated
        :reject_after_transform_if
      ]
    end
  end
end
