# coding: utf-8
require 'bundler/setup'
require 'redis'
require 'hiredis'


# Initialize a connection to Redis.
# Currently the path to redis socket is hardcoded (/tmp/redis.sock),
# as well as the driver (:hiredis).
# global variable $redis gets set.
# 
# 
class RedisConnection
  attr_reader :redis
  def initialize *args
    #@key= key
    $redis ||= Redis.new(:path => "/tmp/redis.sock",:driver => :hiredis)
    @redis = $redis
    #puts green "Connected to Redis"
  end # end initialize
end
