require 'pg'
require 'activerecord-postgres-hstore'

module Anemone
  module Storage
    class Hstore

      MARSHAL_FIELDS = %w(links visited fetched)

      def initialize(opts)
        @table = eval opts[:table_name]
        @key_prefix = opts[:key_prefix]
        @table.where(:key_prefix => @key_prefix).delete_all
      end

      def [](key)
        rget(key.to_s)
      end

      def []=(key,value)
        hash = value.to_hash
        MARSHAL_FIELDS.each do |field|
          hash[field] = Marshal.dump(hash[field])
        end
        if row = @table.find_by_key(key.to_s)
          row.update_attribute(:data, hash)
        else
          @table.create(:key => key.to_s, :key_prefix => @key_prefix, :data => hash)
        end
      end

      def delete(key)
        page = self[key]
        @table.find_by_key(key.to_s).try(:destroy)
        page
      end

      def each
        @table.where(:key_prefix => @key_prefix).find_each do |row|
          hash = row.try(:data)
          if !!hash
            page = load_value(hash)
            yield row.key, page
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
        @table.where(:key_prefix => @key_prefix).count
      end

      def close; end

      private

      def load_value(hash)
        MARSHAL_FIELDS.each do |field|
          unless hash[field].nil? || hash[field] == ''
            hash[field] = Marshal.load(hash[field])
          end
        end
        Page.from_hash(hash)
      end

      def rget(rkey)
        page = @table.where(:key_prefix => @key_prefix, :key => rkey)
        hash = page.try(:data)
        if !!hash
          load_value(hash)
        end
      end

    end
  end
end
