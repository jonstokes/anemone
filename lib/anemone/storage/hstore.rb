require 'pg'
require 'activerecord-postgres-hstore'

module Anemone
  module Storage
    class Hstore

      def initialize(table)
        @table = table
        @table.delete_all
      end

      def [](key)
        row = @table.where("data ? '#{key.to_s}'")
        return row.empty? ? nil : row.first.data[key.to_s]
      end

      def []=(key,value)
        @table.create(:data => {key.to_s => value})
      end

      def delete(key)
        row = @table.where("data ? '#{key.to_s}'")
        page = nil
        unless row.empty?
          page = row.first.data[key.to_s]
          row.first.data[key.to_s].destroy_key(:data, key)
        end
        return page
      end

      def each
        @table.all.each do |row|
          hash = row.data
          hash.each do |key, value|
            yield key, value
          end
        end
      end

      def merge!(hash)
        hash.each  { |key, value| self[key] = value }
        self
      end

      def has_key?
        if self[key].nil?
          return false
        else
          return true
        end
      end

      def size
        @table.all.count
      end

      def close; end

    end
  end
end
