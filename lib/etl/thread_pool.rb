require 'thread'

module ETL
  class ThreadPool
    attr_reader :size, :jobs, :pool

    def initialize(size)
      @size = size
      @jobs = Queue.new
      fill_the_pool
    end

    def reset
      shutdown
      fill_the_pool
    end
    
    def schedule(*args, &block)
      jobs << [block, args]
    end

    def shutdown
      size.times do
        schedule { throw :exit }
      end
      
      pool.map(&:join)
    end

    private

    def fill_the_pool
      @pool = Array.new(size) do |id|
        Thread.new do
          Thread.current[:id] = id + 1

          catch(:exit) do
            loop do
              job, args = jobs.pop
              job.call(*args)
            end
          end
        end
      end
    end

  end
end
