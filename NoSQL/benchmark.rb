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

puts <<-EOS

####################
# INSERT BENCHMARK #
####################
EOS
puts "'set(insert)' benchmark..."
mysql = KvsBenchmarker::MySQLBench.new({
  :host     => options[:host],
  :port     => 3306,
  :database => DB_NAME,
  :username => 'root',
  :table    => TABLE_NAME,
  :sample   => sample
})
mysql.drop_data

hs = KvsBenchmarker::HandlerSocketBench.new({
  :host     => options[:host],
  :port     => HS_RW_PORT,
  :database => DB_NAME,
  :table    => TABLE_NAME,
  :sample   => sample
})

redis = KvsBenchmarker::RedisBench.new({
  :host     => options[:host],
  :port     => MEMCACHE_PORT,
  :sample => sample
})

Benchmark.bm(30) do |rep|
  rep.report("mysql2(single)") do
    mysql.set
  end

  rep.report("HandlerSocket(single)") do
    id = rand(10000)
    hs.set
  end

  redis.drop_data
  rep.report("redis(single)") do
    redis.set
  end
end

mysql.close
hs.close
redis.close

puts <<-EOS

####################
# SELECT BENCHMARK #
####################
EOS
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
  :host   => options[:host],
  :sample => sample
})

puts "Preparing data..."
puts "\t MySQL..."
mysql.setup
puts "\t Redis..."
redis.setup

hs = KvsBenchmarker::HandlerSocketBench.new({
  :host     => options[:host],
  :port     => HS_RO_PORT,
  :database => DB_NAME,
  :table    => TABLE_NAME,
  :sample   => sample
})

Benchmark.bm(30) do |rep|

  rep.report("mysql2") do
    options[:size].times do 
      mysql.get
    end
  end

  rep.report("HandlerSocket(single)") do
    options[:size].times do 
      hs.get
    end
  end

  rep.report("redis(single)") do
    options[:size].times do 
      redis.get
    end
  end
end

mysql.close
hs.close
redis.close


puts <<-EOS

#############################
# PARALLEL SELECT BENCHMARK #
#############################
EOS
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

Benchmark.bm(40) do |rep|
  rep.report("mysql2(#{options[:threads]}process)") do
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

  rep.report("HandlerSocket(#{options[:threads]}process)") do
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

  rep.report("redis(#{options[:threads]}process)") do
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

puts <<-EOS

##########################################
# FOR REPLICATION TEST (By MySQL INSERT) #
##########################################
EOS
mysql = KvsBenchmarker::MySQLBench.new({
  :host     => options[:host],
  :port     => 3306,
  :database => DB_NAME,
  :username => 'root',
  :table    => TABLE_NAME,
  :sample   => sample
})
mysql.drop_data

Benchmark.bm(30) do |rep|
  rep.report("mysql2 insert)") do
    mysql.set
  end
end

mysql.close

puts <<-EOS

##################################################
# FOR REPLICATION TEST (By HandlerSocket INSERT) #
##################################################
EOS
mysql = KvsBenchmarker::MySQLBench.new({
  :host     => options[:host],
  :port     => 3306,
  :database => DB_NAME,
  :username => 'root',
  :table    => TABLE_NAME,
  :sample   => sample
})
mysql.drop_data
mysql.close

Benchmark.bm(30) do |rep|
  rep.report("HS insert") do
    hs_1 = KvsBenchmarker::HandlerSocketBench.new({
      :host     => options[:host],
      :port     => HS_RW_PORT,
      :database => DB_NAME,
      :table    => TABLE_NAME,
      :sample   => sample
    })
    hs_1.set
    hs_1.close
  end
end

puts <<-EOS

#########################################################
# COMPLEX BENCHMARK(MySQL Insert + HandlerSocket SELECT #
#########################################################
EOS
mysql = KvsBenchmarker::MySQLBench.new({
  :host     => options[:host],
  :port     => 3306,
  :database => DB_NAME,
  :username => 'root',
  :table    => TABLE_NAME,
  :sample   => sample
})

mysql.drop_data

hs = KvsBenchmarker::HandlerSocketBench.new({
  :host     => options[:host],
  :port     => HS_RO_PORT,
  :database => DB_NAME,
  :table    => TABLE_NAME,
  :sample   => sample
})
Benchmark.bm(30) do |rep|
  rep.report("mysql2 + HS") do
    puts
    Process.fork() {
      begin
        mysql.set
      rescue
        puts "mysql2 error occurred"
      end
    }
    Process.fork() {
      options[:size].times do |n|
        begin
          hs.get_first_record_slowly
        rescue
          puts "handlersocket error occurred : #{n}"
        end
      end
    }
    Process.waitall
  end
end
mysql.close
hs.close

puts <<-EOS

#####################
# COMPLEX BENCHMARK #
#####################
EOS
mysql = KvsBenchmarker::MySQLBench.new({
  :host     => options[:host],
  :port     => 3306,
  :database => DB_NAME,
  :username => 'root',
  :table    => TABLE_NAME,
  :sample   => sample
})
mysql.drop_data
mysql.close

Benchmark.bm(30) do |rep|
  rep.report("HS + HS") do
    hs_1 = KvsBenchmarker::HandlerSocketBench.new({
      :host     => options[:host],
      :port     => HS_RW_PORT,
      :database => DB_NAME,
      :table    => TABLE_NAME,
      :sample   => sample
    })
    Process.fork() { hs_1.set }

    hs_2 = KvsBenchmarker::HandlerSocketBench.new({
      :host     => options[:host],
      :port     => HS_RO_PORT,
      :database => DB_NAME,
      :table    => TABLE_NAME,
      :sample   => sample
    })
    Process.fork() { options[:size].times { hs_2.get } }

    Process.waitall
    hs_1.close
    hs_2.close
  end
end

