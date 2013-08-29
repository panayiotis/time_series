#!/usr/bin/ruby
# http://mattsears.com/articles/2011/12/10/minitest-quick-reference

require 'bundler/setup'
require 'redis'
require_relative "../lib/time_series"

require "minitest/autorun"
require 'minitest/pride'

require 'active_support/core_ext/numeric/time.rb'
require 'active_support/core_ext/date/calculations.rb'

describe "TimeSeries" do

  before do
    $app_prefix="test"
    @redis ||= Redis.new(:path => "/tmp/redis.sock")
    @redis.keys("test:*").each{|k| @redis.del k}
  end

   after do
    @redis.keys("test:*").each{|k| @redis.del k}
  end  

  it "can separate older items from an array" do
    ts = TimeSeries.new("test_ts")
    array = Array.new
    # create 7 days
    (0..6).each do |i|
      array << ts.time_to_key(Time.now - i.days)
    end

    ts.array_older_than(array, 3.days).must_be_instance_of Array
    ts.array_older_than(array, 3.days).size.must_equal 4

  end

  it "can separate seconds, minutes,hours,... keys" do
    ts = TimeSeries.new("test_ts")

    # all methods must return empty arrays
    ts.seconds.must_be_instance_of Array
    ts.minutes.must_be_instance_of Array
    ts.hours.must_be_instance_of Array
    ts.days.must_be_instance_of Array
    ts.months.must_be_instance_of Array
    ts.years.must_be_instance_of Array
    
    # all arrays are empty
    ts.seconds.size.must_equal 0
    ts.minutes.size.must_equal 0
    ts.hours.size.must_equal 0
    ts.days.size.must_equal 0
    ts.months.size.must_equal 0
    ts.years.size.must_equal 0

    # seconds
    @redis.zincrby "test:ts:test_ts:1986:12:30:23:59:01", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:30:23:59:02", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:30:23:59:03", 1, "one"
    ts.seconds.size.must_equal 3

    # minutes
    @redis.zincrby "test:ts:test_ts:1986:12:30:23:59", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:30:23:58", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:30:23:57", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:30:23:56", 1, "one"
    ts.minutes.size.must_equal 4   

    # hours
    @redis.zincrby "test:ts:test_ts:1986:12:30:23", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:30:22", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:30:21", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:30:20", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:30:19", 1, "one"    
    ts.hours.size.must_equal 5

    # days
    @redis.zincrby "test:ts:test_ts:1986:12:30", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:29", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:28", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:27", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:12:26", 1, "one"   
    @redis.zincrby "test:ts:test_ts:1986:12:25", 1, "one"         
    ts.days.size.must_equal 6

    # months
    @redis.zincrby "test:ts:test_ts:1986:12", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:11", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:10", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:09", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:08", 1, "one"   
    @redis.zincrby "test:ts:test_ts:1986:07", 1, "one"      
    @redis.zincrby "test:ts:test_ts:1986:06", 1, "one"                
    ts.months.size.must_equal 7

    # years
    @redis.zincrby "test:ts:test_ts:1986", 1, "one"
    @redis.zincrby "test:ts:test_ts:1985", 1, "one"
    @redis.zincrby "test:ts:test_ts:1984", 1, "one"
    @redis.zincrby "test:ts:test_ts:1983", 1, "one"
    @redis.zincrby "test:ts:test_ts:1982", 1, "one"   
    @redis.zincrby "test:ts:test_ts:1981", 1, "one"   
    @redis.zincrby "test:ts:test_ts:1980", 1, "one"         
    @redis.zincrby "test:ts:test_ts:1979", 1, "one"                       
    ts.years.size.must_equal 8
    
    # check everything is still as expected
    ts.seconds.size.must_equal 3
    ts.minutes.size.must_equal 4
    ts.hours.size.must_equal 5
    ts.days.size.must_equal 6
    ts.months.size.must_equal 7

    # make some keys volatile, volatile keys are not included
    @redis.expire "test:ts:test_ts:1986:12:30:23:59:01", 10
    @redis.expire "test:ts:test_ts:1986:12:30:23:59", 10
    @redis.expire "test:ts:test_ts:1986:12:30:23", 10
    @redis.expire "test:ts:test_ts:1986:12:30", 10
    @redis.expire "test:ts:test_ts:1986:12", 10
    @redis.expire "test:ts:test_ts:1986", 10
    
    ts.seconds.size.must_equal 2
    ts.minutes.size.must_equal 3
    ts.hours.size.must_equal 4
    ts.days.size.must_equal 5
    ts.months.size.must_equal 6
    ts.years.size.must_equal 7        

  end


  it "can separate seconds, minutes,hours,... keys OLDER THAN" do
    ts = TimeSeries.new("test_ts")
    
    # create 7 seconds one day apart
    (0..6).each do |i|
      @redis.zincrby ts.time_to_key(Time.now - i.days), 1, "one"
    end
    ts.seconds(3.days).size.must_equal 4

    # create 10 hours
    (0..9).each do |i|
      @redis.zincrby ts.time_to_key(Time.now - i.days).parent.parent, 1, "one"
    end
    ts.hours(1.hours).size.must_equal 9
        
    # create 3 days
    (0..2).each do |i|
      @redis.zincrby ts.time_to_key(Time.now - i.days).parent.parent.parent, 1, "one"
    end
    ts.days(1.days).size.must_equal 2

  end

  it "can compress non recent seconds to minutes to hours" do
    ts = TimeSeries.new("test_ts")
    
    # create 10 seconds 30 seconds apart
    (0..9).each do |i|
      @redis.zincrby ts.time_to_key(Time.local(2000, 7, 31)  - 30*i.seconds), 1, "one"
    end

    ts.keys.size.must_equal 10
    ts.compress(ts.seconds).must_equal 10 # 10 seconds must be compressed
    ts.keys.size.wont_equal 10
    minutes_num = ts.keys.size
    ts.keys.each { |k| k.minute?.must_equal true} # all keys must be minute keys
    ts.compress(ts.minutes).must_equal minutes_num # all must be compressed
    ts.keys.each { |k| k.hour?.must_equal true} # all keys must be hour keys
  end
  
  it "can compress non recent hours to days to months" do
    ts = TimeSeries.new("test_ts")  
    # create 10 hours 6 hours apart
    (0..9).each do |i|
      @redis.zincrby ts.time_to_key(Time.local(2000, 7, 31) - 6*i.hours).parent.parent, 1, "one"
    end

    # create 10 minutes 6 hours apart on the year 2001
    (0..9).each do |i|
      @redis.zincrby ts.time_to_key(Time.local(2001, 7, 31) - 6*i.hours).parent, 1, "one"
    end

    ts.compress(ts.minutes).must_equal 10
    ts.keys.each { |k| k.hour?.must_equal true} # all keys must be hour keys
    ts.compress(ts.hours).must_equal 20
    ts.keys.each { |k| k.day?.must_equal true} # all keys must be day keys
    ts.compress ts.days
    ts.keys.each { |k| k.month?.must_equal true} # all keys must be month keys
    ts.compress ts.months
    ts.keys.each { |k| k.year?.must_equal true} # all keys must be year keys
    ts.keys.size.must_equal 2
  end

  it "can compress a mixture of minute, hour, day keys" do
    ts = TimeSeries.new("test_ts")
    
    # create 10 hours 6 hours apart
    (0..9).each do |i|
      @redis.zincrby ts.time_to_key(Time.local(2000, 7, 31) - 6*i.hours).parent.parent, 1, "one"
    end

    # create 10 minutes 6 hours apart on the year 2001
    (0..9).each do |i|
      @redis.zincrby ts.time_to_key(Time.local(2001, 7, 31) - 6*i.hours).parent, 1, "one"
    end

    # create 10 days 1 day apart on the year 2002
    (0..9).each do |i|
      @redis.zincrby ts.time_to_key(Time.local(2002, 7, 31) - i.days).parent.parent.parent, 1, "one"
    end

    ts.compress ts.keys
    ts.compress ts.keys
    ts.compress ts.keys
    ts.compress ts.keys
    ts.keys.each { |k| k.year?.must_equal true} # all keys must be year keys
    ts.keys.size.must_equal 3

  end

  it "can compress a mixture of keys but will exclude recent keys" do
    ts = TimeSeries.new("test_ts")  
    
    max_hour = Time.now.hour
    max_min = Time.now.min
    max_sec = Time.now.sec
    max_day = Time.now.day

    # create seconds
    (1..max_sec).each do |i|
      @redis.zincrby ts.time_to_key(Time.now - i), 1, "one"
    end    

    # create hours
    (1...max_hour).each do |i|
      @redis.zincrby ts.time_to_key(Time.now - i.hours).parent.parent, 1, "one"
    end

    # create minutes
    (1..max_min).each do |i|
      @redis.zincrby ts.time_to_key(Time.now - i.minutes).parent, 1, "one"
    end

    # create days
    (1...max_day).each do |i|
      @redis.zincrby ts.time_to_key(Time.now - i.days).parent.parent.parent, 1, "one"
    end

    # create months
    (1...max_day).each do |i|
      @redis.zincrby ts.time_to_key(Time.now - i.days).parent.parent.parent, 1, "one"
    end   
     
    recent_keys = ts.keys
    ts.compress ts.keys
    ts.compress ts.keys
    ts.compress ts.keys
    ts.compress ts.keys
    ts.keys.must_equal recent_keys # nothing must be compressed
    
    # Now create a mixture of non recent keys

    # create non recent hours
    (1...max_hour).each do |i|
      @redis.zincrby ts.time_to_key(Time.local(2002, 7, 31) - i.hours).parent.parent, 1, "one"
    end
    
    all_keys = ts.keys
    ts.compress ts.keys
    ts.compress ts.keys
    ts.compress ts.keys
    ts.compress ts.keys
    ts.keys.wont_equal all_keys # something must be compressed
    "test:ts:test_ts:2002".exists?.must_equal true
  end
    
  it "remove terms by score" do
    ts = TimeSeries.new("test_ts")

    @redis.zincrby "test:ts:test_ts:1986", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986", 2, "two"
    @redis.zincrby "test:ts:test_ts:1986", 3, "three"
    @redis.zincrby "test:ts:test_ts:1986", 4, "four"
    @redis.zincrby "test:ts:test_ts:1986", 5, "five"
    @redis.zincrby "test:ts:test_ts:1986", 6, "six"            

    @redis.zincrby "test:ts:test_ts:1987", 1, "one"
    @redis.zincrby "test:ts:test_ts:1987", 2, "two"
    @redis.zincrby "test:ts:test_ts:1987", 3, "three"
    @redis.zincrby "test:ts:test_ts:1987", 4, "four"
    @redis.zincrby "test:ts:test_ts:1987", 5, "five"
    @redis.zincrby "test:ts:test_ts:1987", 6, "six"

    @redis.zincrby "test:ts:test_ts:1988:01", 1, "one"
    @redis.zincrby "test:ts:test_ts:1988:01", 2, "two"
    @redis.zincrby "test:ts:test_ts:1988:01", 3, "three"
    @redis.zincrby "test:ts:test_ts:1988:01", 4, "four"
    @redis.zincrby "test:ts:test_ts:1988:01", 5, "five"
    @redis.zincrby "test:ts:test_ts:1988:01", 6, "six"     
      
    ts.remove_by_score ts.keys, 4
    
    expected = { "five" => 5, "six" => 6 }
    "test:ts:test_ts:1986".get.must_equal expected
    "test:ts:test_ts:1987".get.must_equal expected
    "test:ts:test_ts:1988:01".get.must_equal expected
  end      
end
