require File.dirname(File.expand_path(__FILE__)) + '/../carrot'

#Carrot.logging = true
all_consumer = Carrot.queue('all')
bar_consumer = Carrot.queue('bar')

topic = Carrot.topic("nomnom")

bar_consumer.bind(topic, :key => "foo.bar")
all_consumer.bind(topic, :key => "foo.*")

topic.publish('foo', :key => "foo.bar")
topic.publish('baz', :key => "foo.baz")

msg = bar_consumer.pop(:ack => true)
puts "bar consumer: #{msg}"

while msg = all_consumer.pop(:ack => true)
  puts "all consumer: #{msg}"
end

Carrot.stop
