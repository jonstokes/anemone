require 'pg'
require 'activerecord-postgres-hstore'

module Anemone
  module Storage
    class Hstore

      def initialize(opts)
        @table = eval opts[:table_name]
        @key_prefix = opts[:key_prefix]
        @table.delete_all
      end

      def [](key)
        key = @key_prefix + key.to_s
        row = @table.where("data ? '#{key}'")
        return row.empty? ? nil : row.first.data[key]
      end

      def []=(key,value)
        key = @key_prefix + key.to_s
        @table.create(:data => {key => value})
      end

      def delete(key)
        key = @key_prefix + key.to_s
        row = @table.where("data ? '#{key}'")
        page = nil
        unless row.empty?
          page = row.first.data[key]
          row.first.data[key].destroy_key(:data, key)
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

      def keys
        keys = []
        self.each { |k, v| keys << k.to_s }
        keys
      end

      def merge!(hash)
        hash.each  { |key, value| self[key] = value }
        self
      end

      def has_key?(key)
        key = @key_prefix + key.to_s
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
