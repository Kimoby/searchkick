module Searchkick
  class ReindexQueue
    attr_reader :name

    def initialize(name)
      @name = name

      raise Searchkick::Error, "Searchkick.redis not set" unless Searchkick.redis
    end

    def push(record_id)
      Searchkick.with_redis { |r| r.sadd(redis_key, record_id) }
    end

    # TODO use reliable queuing
    def reserve(limit: 1000)
      Searchkick.with_redis { |r| r.spop(redis_key, limit) }
    end

    def clear
      Searchkick.with_redis { |r| r.del(redis_key) }
    end

    def length
      Searchkick.with_redis { |r| r.scard(redis_key) }
    end

    private

    def redis_key
      "searchkick:reindex_queue:#{name}"
    end
  end
end
