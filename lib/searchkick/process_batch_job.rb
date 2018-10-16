module Searchkick
  class ProcessBatchJob < ActiveJob::Base
    queue_as { Searchkick.queue_name }

    def perform(class_name:, record_ids:, index_name: nil)
      # separate routing from id
      routing = Hash[record_ids.map { |r| r.split(/(?<!\|)\|(?!\|)/, 2).map { |v| v.gsub("||", "|") } }]
      record_ids = routing.keys

      klass = class_name.constantize
      scope = Searchkick.load_records(klass, record_ids)
      scope = scope.search_import if scope.respond_to?(:search_import)
      records = scope.select(&:should_index?)

      # determine which records to delete
      delete_ids = record_ids - records.map { |r| r.id.to_s }
      delete_records = klass.with_deleted.where(id: delete_ids)

      delete_records.each do |record|
        if routing[record.id]
          record.define_singleton_method(:search_routing) do
            routing[record.id]
          end
        end
      end

      # bulk reindex
      index = klass.searchkick_index(name: index_name)
      Searchkick.callbacks(:bulk) do
        index.bulk_index(records) if records.any?
        index.bulk_delete(delete_records) if delete_records.any?
      end
    end
  end
end
