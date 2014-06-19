require 'pp'
require 'benchmark'
require 'optparse'
require './lib/kvs_benchmarker'

DB_NAME       = 'test'
TABLE_NAME    = 'hs_test'
HS_RO_PORT       = '9998'
HS_RW_PORT       = '9999'
MEMCACHE_PORT = '11211'

STDOUT.sync = true

options = {}
OptionParser.new do |o|
  o.banner = "Usage ruby #{File.basename($0)} [OPTIOINS]"
  o.separator ''
  begin
    required = [ :size, :host ]
    o.on('--size size', 'size of records.(must)', Integer){|size|
      raise ArgumentError, '--size must be specified!(>1)' if size < 0
      options[:size] = size
    }

    o.on('--threads num', 'num of records.', Integer){|num|
      options[:threads] = num
    }

    o.on('--host host', 'KVS host.(must)'){|host|
      raise ArgumentError, '--host must be specified!' if host.empty?
      options[:host] = host
    }

    o.parse!(ARGV)

    required.each do |r| 
      raise ArgumentError, 'lack options.' unless options.key?(r)
    end

    options[:threads] = 1 if options[:threads].nil? || options[:threads] <= 0

  rescue OptionParser::InvalidArgument, OptionParser::InvalidOption, ArgumentError => e
    print <<-EOS
#{e.message}
#{o}
EOS
    exit
  end
end

sample = KvsBenchmarker::Sample.new({:size => options[:size]})

# ####################
# # INSERT BENCHMARK #
# ####################
# puts "'set(insert)' benchmark..."
# mysql = KvsBenchmarker::MySQLBench.new ({
#   :host     => options[:host],
#   :port     => 3306,
#   :database => DB_NAME,
#   :username => 'root',
#   :table    => TABLE_NAME,
#   :sample   => sample
# })
# 
# hs = KvsBenchmarker::HandlerSocketBench.new({
#   :host     => options[:host],
#   :port     => HS_RW_PORT,
#   :database => DB_NAME,
#   :table    => TABLE_NAME,
#   :sample   => sample
# })
# 
# redis = KvsBenchmarker::RedisBench.new({
#   :host     => options[:host],
#   :port     => MEMCACHE_PORT,
#   :sample => sample
# })
# 
# Benchmark.bm(30) do |rep|
#   mysql.drop_data
#   rep.report("mysql2") do
#     mysql.set
#   end
# 
#   mysql.drop_data
#   rep.report("HandlerSocket(single)") do
#     id = rand(10000)
#     hs.set
#   end
# 
#   redis.drop_data
#   rep.report("redis(single)") do
#     redis.set
#   end
# end
# 
# mysql.close
# hs.close
# redis.close

# ####################
# # SELECT BENCHMARK #
# ####################
# puts "get benchmark..."
# mysql = KvsBenchmarker::MySQLBench.new ({
#   :host     => options[:host],
#   :port     => 3306,
#   :database => DB_NAME,
#   :username => 'root',
#   :table    => TABLE_NAME,
#   :sample   => sample
# })
# 
# redis = KvsBenchmarker::RedisBench.new({
#   :host   => options[:host],
#   :sample => sample
# })
# 
# puts "Preparing data..."
# puts "\t MySQL..."
# mysql.setup
# puts "\t Redis..."
# redis.setup
# 
# hs = KvsBenchmarker::HandlerSocketBench.new({
#   :host     => options[:host],
#   :port     => HS_RO_PORT,
#   :database => DB_NAME,
#   :table    => TABLE_NAME,
#   :sample   => sample
# })
# 
# Benchmark.bm(30) do |rep|
# 
#   rep.report("mysql2") do
#     options[:size].times do 
#       mysql.get
#     end
#   end
# 
#   rep.report("HandlerSocket(single)") do
#     options[:size].times do 
#       hs.get
#     end
#   end
# 
#   rep.report("redis(single)") do
#     options[:size].times do 
#       redis.get
#     end
#   end
# end
# 
# mysql.close
# hs.close
# redis.close


#############################
# PARALLEL SELECT BENCHMARK #
#############################
puts "get benchmark..."
mysql = KvsBenchmarker::MySQLBench.new({
  :host     => options[:host],
  :port     => 3306,
  :database => DB_NAME,
  :username => 'root',
  :table    => TABLE_NAME,
  :sample   => sample
})

redis = KvsBenchmarker::RedisBench.new({
  :host     => options[:host],
  :sample => sample
})

puts "Preparing data..."
puts "\t MySQL..."
mysql.setup
puts "\t Redis..."
redis.setup

mysql.close
redis.close

print "Wait... "
10.downto(1) do |i|
  print "#{i} "
  sleep 1
end
puts

Benchmark.bm(20) do |rep|
  rep.report("mysql2") do
    options[:threads].times do |n|
      Process.fork() {
        mysql = KvsBenchmarker::MySQLBench.new({
          :host     => options[:host],
          :port     => 3306,
          :database => DB_NAME,
          :username => 'root',
          :table    => TABLE_NAME,
          :sample   => sample
        })

        (options[:size]/options[:threads]).times do
          mysql.get
        end
      }
    end

    Process.waitall
  end

  rep.report("HandlerSocket(single)") do
    options[:threads].times do |n|
      Process.fork() {
        hs = KvsBenchmarker::HandlerSocketBench.new({
          :host     => options[:host],
          :port     => HS_RO_PORT,
          :database => DB_NAME,
          :table    => TABLE_NAME,
          :sample   => sample
        })

        (options[:size]/options[:threads]).times do
          hs.get
        end

        hs.close
      }
    end

    Process.waitall
  end

  rep.report("redis(single)") do
    options[:threads].times do |n|
      Process.fork() {
        redis = KvsBenchmarker::RedisBench.new({
          :host     => options[:host],
          :port     => MEMCACHE_PORT,
          :sample => sample
        })

        (options[:size]/options[:threads]).times do
          redis.get 
        end

        redis.close
      }
    end

    Process.waitall
  end
end

