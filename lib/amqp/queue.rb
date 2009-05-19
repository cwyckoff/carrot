require 'ruby-debug'

module Carrot::AMQP
  class Queue
    attr_reader :name, :server, :carrot
    attr_accessor :delivery_tag

    def initialize(carrot, name, opts = {})
      @server = carrot.server
      @opts   = opts
      @name   = name
      @carrot = carrot
      server.send_frame(
        Protocol::Queue::Declare.new({ :queue => name, :nowait => true }.merge(opts))
      )
    end

    def pop(opts = {})
      self.delivery_tag = nil
      server.send_frame(
        Protocol::Basic::Get.new({ :queue => name, :consumer_tag => name, :no_ack => !opts.delete(:ack), :nowait => true }.merge(opts))
      )
      method = server.next_method
      return unless method.is_a?(Protocol::Basic::GetOk)

      self.delivery_tag = method.delivery_tag

      header = server.next_payload
      msg    = server.next_payload
      raise 'unexpected length' if msg.length < header.size

      msg
    end

    def subscribe(opts = {}, &blk)
      consumer_tag = opts[:consumer_tag] || name 
      opts.delete(:nowait)
      hdr = opts.delete(:header)

      server.send_frame(Protocol::Basic::Consume.new({ :queue => name,
                                                       :consumer_tag => consumer_tag,
                                                       :no_ack => !opts.delete(:ack),
                                                       :nowait => false }.merge(opts)) )

      raise "Error subscribing to queue #{name}" unless
        server.next_method.is_a?(Protocol::Basic::ConsumeOk)

      while true
        method = server.next_method
        self.delivery_tag = method.delivery_tag
        header = server.next_payload
        msg    = server.next_payload
        raise 'unexpected length' if msg.length < header.size

        blk.call(hdr ? {:header => header, :payload => msg, :delivery_details => method.arguments} : msg)
      end 
    end

    def ack
      server.send_frame(
        Protocol::Basic::Ack.new(:delivery_tag => delivery_tag)
      )
    end

    def publish(data, opts = {})
      exchange.publish(data, opts)
    end

    def message_count
      status.first
    end

    def consumer_count
      status.last
    end
    
    def status(opts = {}, &blk)
      server.send_frame(
        Protocol::Queue::Declare.new({ :queue => name, :passive => true }.merge(opts))
      )
      method = server.next_method
      [method.message_count, method.consumer_count]
    end

    def bind(exchange, opts = {})
      exchange           = exchange.respond_to?(:name) ? exchange.name : exchange
      bindings[exchange] = opts
      server.send_frame(
        Protocol::Queue::Bind.new({ :queue => name, :exchange => exchange, :routing_key => opts.delete(:key), :nowait => true }.merge(opts))
      )
    end

    def unbind(exchange, opts = {})
      exchange = exchange.respond_to?(:name) ? exchange.name : exchange
      bindings.delete(exchange)
      server.send_frame(
        Protocol::Queue::Unbind.new({
          :queue => name, :exchange => exchange, :routing_key => opts.delete(:key), :nowait => true }.merge(opts)
        )
      )
    end

    def delete(opts = {})
      server.send_frame(
        Protocol::Queue::Delete.new({ :queue => name, :nowait => true }.merge(opts))
      )
      carrot.queues.delete(name)
    end

  private
    def exchange
      @exchange ||= Exchange.new(carrot, :direct, '', :key => name)
    end

    def bindings
      @bindings ||= {}
    end
  end
end
