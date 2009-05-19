require File.dirname(File.expand_path(__FILE__)) + '/../carrot'

# run this consumer in conjunction with the publisher script in 'direct_publisher.rb'
# by starting this consumer in one terminal, and the publisher script in another.
# the message "foo" should appear in the consumer's terminal each time the publisher
# is run. 

# Carrot.logging = true
q = Carrot.queue('direct_example', :durable => true)

puts "listening for messages on queue 'direct_example'..."

q.subscribe(:ack => true) do |msg|
  puts "received message: #{msg}"
  q.ack
end

Carrot.stop
