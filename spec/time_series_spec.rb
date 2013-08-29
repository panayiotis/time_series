#!/usr/bin/ruby
# http://mattsears.com/articles/2011/12/10/minitest-quick-reference

require 'bundler/setup'
require_relative "../lib/time_series"
require "minitest/autorun"
require 'minitest/pride'

describe "TimeSeries" do

  before do
    $app_prefix="test"
    @redis ||= Redis.new(:path => "/tmp/redis.sock")
    @redis.keys("test:*").each{|k| @redis.del k}
  end

   after do
    @redis.keys("test:*").each{|k| @redis.del k}
  end  

  it "gets created with default resolution" do
    @ts = TimeSeries.new "test_ts"
    @ts.name.must_equal "test_ts"
    @ts.resolution.must_equal :second
  end

  it "gets created with resolution guessed from existing keys" do
    @redis.zincrby "test:ts:test_ts:1986:11:04:12", 2, "one"  # hour
    @redis.zincrby "test:ts:test_ts:1986:11:04", 2, "one"     # day
    @redis.zincrby "test:ts:test_ts:1986:11", 2, "one"        # month
    @ts = TimeSeries.new "test_ts"
    @ts.resolution.must_equal :hour
  end
  
  it "throws error on wrong resolution option given" do
    proc { TimeSeries.new("test_ts", :resolution => :hello ) }.must_raise ArgumentError
  end  
  
  it "generates current_key corectly" do
    @sec_ts = TimeSeries.new "sec_ts"
    @min_ts = TimeSeries.new "min_ts", :resolution => :minute
    @day_ts = TimeSeries.new "day_ts", :resolution => :day
    @sec_ts.current_key.resolution.must_equal :second
    @min_ts.current_key.resolution.must_equal :minute
    @day_ts.current_key.resolution.must_equal :day        

    TimeSeries.new("ts", :resolution => :hour).current_key.must_equal Time.now.strftime("test:ts:ts:%Y:%m:%d:%H") 
    
   # sec = Time.now.strftime("%S").to_i / 5 * 5
   # TimeSeries.new("ts").current_key.must_equal Time.now.strftime("test:ts:ts:%Y:%m:%d:%H:%M:#{"%01d" % sec}")    
  end


  it "generates correct times" do
    ts = TimeSeries.new "sec_ts"
    duration = ts.duration
    now = Time.now
    
    #ap now
    #ap ts.current_time
    #ap ts.last_time
    #ap ts.previous_time
    (now - ts.current_time).must_be :<, duration    
    (ts.current_time - ts.last_time).must_be :==, duration
    (ts.current_time - ts.previous_time).must_be :==, 2*duration
  end

  it "has keys" do
    i = 0
    while i < 60  do
      @redis.zincrby "test:ts:test_ts:1986:11:04:12:00:#{i}", 2, "one"  # second
      i+=1
    end
    TimeSeries.new("test_ts").keys.size.must_equal 60
  end

  it "clears data" do
    ts = TimeSeries.new("test_ts")
    i = 0
    while i < 60  do
      @redis.zincrby "test:ts:test_ts:1986:11:04:12:00:#{i}", 2, "one"  # second
      i+=1
    end
    ts.keys.size.must_equal 60  
    ts.clear
    ts.keys.size.must_equal 0
  end

  
  it "gets all keys in a hash" do
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:01", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:02", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:05", 2, "one"

    ts = TimeSeries.new("test_ts")
    ts.all.wont_be_empty
    ts.all.must_be_instance_of Hash
  end
  
  it "gets last keys" do
    @redis.zincrby "test:ts:test_ts:2013:01", 2, "one"  
    
    # the ts has month resolution
    ts = TimeSeries.new("test_ts")
    ts.last.wont_be_empty
    ts.last.must_be_instance_of Hash
  end
  
  it "gets previous keys" do
    @redis.zincrby "test:ts:test_ts:2013:01", 2, "one"  
    
    # the ts has month resolution
    ts = TimeSeries.new("test_ts")
    ts.previous.wont_be_empty
    ts.previous.must_be_instance_of Hash
  end  
  
  it "adds terms" do
    ts = TimeSeries.new("test_ts")  
    ts.push "hello"
    @redis.zscore(ts.current_key, "hello").wont_be_nil
  end
  
    
end
