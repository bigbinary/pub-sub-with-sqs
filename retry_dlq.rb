#!/usr/bin/env ruby

require 'aws-sdk-sqs'
require 'logger'
require 'optparse'
require 'json'
require 'securerandom'

# Parse command line arguments
options = {
  dlq_url: nil,                 # Source DLQ URL (required)
  destination_url: nil,         # Destination queue URL (required)
  batch_size: 10,               # Number of messages to process in each batch
  delay_seconds: 0,             # Delay before message becomes visible in destination queue
  max_messages: nil,            # Maximum number of messages to process (nil = all available)
  delete_after_send: true,      # Whether to delete messages from DLQ after successful send
  wait_time: 10,                # Long polling wait time
  region: 'us-east-1',          # AWS region
  processor_id: "dlq-retry-#{SecureRandom.hex(4)}" # Unique identifier for this processor
}

# Parse command line options
OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [options]"
  
  opts.on('-d', '--dlq-url URL', 'Dead Letter Queue URL (required)') do |url|
    options[:dlq_url] = url
  end
  
  opts.on('-q', '--destination-url URL', 'Destination Queue URL (required)') do |url|
    options[:destination_url] = url
  end
  
  opts.on('-b', '--batch-size NUM', Integer, 'Number of messages to process in each batch (default: 10, max: 10)') do |num|
    options[:batch_size] = [num, 10].min # SQS maximum batch size is 10
  end
  
  opts.on('-s', '--delay-seconds NUM', Integer, 'Delay in seconds before messages become visible in destination (default: 0)') do |num|
    options[:delay_seconds] = num
  end
  
  opts.on('-m', '--max-messages NUM', Integer, 'Maximum number of messages to process (default: all available)') do |num|
    options[:max_messages] = num
  end
  
  opts.on('-k', '--keep-in-dlq', 'Keep messages in DLQ after sending (default: delete after successful send)') do
    options[:delete_after_send] = false
  end
  
  opts.on('-w', '--wait-time SECONDS', Integer, 'Long polling wait time in seconds (default: 10)') do |sec|
    options[:wait_time] = sec
  end
  
  opts.on('-r', '--region REGION', 'AWS Region (default: us-east-1)') do |region|
    options[:region] = region
  end
  
  opts.on('-i', '--id IDENTIFIER', 'Processor identifier (default: random ID)') do |id|
    options[:processor_id] = id
  end
  
  opts.on('-h', '--help', 'Display this help message') do
    puts opts
    exit
  end
end.parse!

# Validate required parameters
if options[:dlq_url].nil?
  puts "Error: DLQ URL is required. Use --dlq-url option."
  exit(1)
end

if options[:destination_url].nil?
  puts "Error: Destination Queue URL is required. Use --destination-url option."
  exit(1)
end

# Custom log formatter that includes processor ID
class RetryFormatter < Logger::Formatter
  def initialize(processor_id)
    @processor_id = processor_id
  end
  
  def call(severity, time, progname, msg)
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S.%L")
    "[#{formatted_time}] [#{severity}] [#{@processor_id}] #{msg}\n"
  end
end

# Initialize logger with custom formatter
logger = Logger.new(STDOUT)
logger.level = Logger::INFO
logger.formatter = RetryFormatter.new(options[:processor_id])

# Log configuration details
logger.info("Starting DLQ retry processor")
logger.info("Source DLQ: #{options[:dlq_url]}")
logger.info("Destination Queue: #{options[:destination_url]}")
logger.info("Batch size: #{options[:batch_size]}")
logger.info("Delay seconds: #{options[:delay_seconds]}")
logger.info("Max messages: #{options[:max_messages] || 'all available'}")
logger.info("Delete after send: #{options[:delete_after_send]}")
logger.info("Region: #{options[:region]}")
logger.info("Processor ID: #{options[:processor_id]}")

begin
  # Initialize AWS SQS client
  sqs = Aws::SQS::Client.new(region: options[:region])
  logger.info("Connected to SQS in region #{options[:region]}")
  
  # Counters for statistics
  stats = {
    messages_received: 0,
    messages_sent: 0,
    messages_failed: 0,
    batches_processed: 0
  }
  
  # Setup signal handler for graceful shutdown
  running = true
  Signal.trap('INT') do
    logger.info('Received interrupt signal. Shutting down gracefully...')
    running = false
  end
  
  # Main processing loop
  logger.info("Starting message processing loop")
  
  while running
    # Check if we've reached the maximum number of messages to process
    if options[:max_messages] && stats[:messages_sent] >= options[:max_messages]
      logger.info("Reached maximum number of messages to process (#{options[:max_messages]})")
      break
    end
    
    # Calculate how many messages to request in this batch
    remaining = options[:max_messages] ? options[:max_messages] - stats[:messages_sent] : options[:batch_size]
    batch_size = [remaining, options[:batch_size]].min
    
    # Exit if we've processed all requested messages
    break if batch_size <= 0
    
    # Receive messages from DLQ
    begin
      receive_response = sqs.receive_message(
        queue_url: options[:dlq_url],
        max_number_of_messages: batch_size,
        wait_time_seconds: options[:wait_time],
        attribute_names: ['All'],
        message_attribute_names: ['All']
      )
      
      # Check if we received any messages
      if receive_response.messages.empty?
        logger.info("No messages received from DLQ")
        
        # If we're looking for a specific number of messages and there are none, we're done
        if options[:max_messages]
          logger.info("No more messages available in DLQ")
          break
        end
        
        # Otherwise, continue the loop to try again
        next
      end
      
      # Update statistics
      stats[:messages_received] += receive_response.messages.size
      stats[:batches_processed] += 1
      
      logger.info("Received #{receive_response.messages.size} messages from DLQ (batch #{stats[:batches_processed]})")
      
      # Process each message - send to destination queue
      successful_receipts = []
      
      receive_response.messages.each do |message|
        begin
          # Send message to destination queue
          send_response = sqs.send_message(
            queue_url: options[:destination_url],
            message_body: message.body,
            delay_seconds: options[:delay_seconds],
            message_attributes: message.message_attributes || {}
          )
          
          # If successful, add to deletion list (if deletion is enabled)
          if send_response.message_id
            logger.info("Successfully sent message #{message.message_id} to destination queue (new ID: #{send_response.message_id})")
            successful_receipts << message.receipt_handle if options[:delete_after_send]
            stats[:messages_sent] += 1
          end
        rescue Aws::SQS::Errors::ServiceError => e
          logger.error("Failed to send message #{message.message_id}: #{e.message}")
          stats[:messages_failed] += 1
        end
      end
      
      # Delete successfully processed messages from DLQ if requested
      if options[:delete_after_send] && !successful_receipts.empty?
        # SQS only allows deleting up to 10 messages in a batch
        successful_receipts.each_slice(10) do |receipt_handles|
          delete_entries = receipt_handles.map.with_index do |receipt, index|
            {
              id: index.to_s,
              receipt_handle: receipt
            }
          end
          
          begin
            delete_response = sqs.delete_message_batch(
              queue_url: options[:dlq_url],
              entries: delete_entries
            )
            
            if delete_response.successful.size != delete_entries.size
              failed = delete_response.failed.map { |f| "#{f.id}: #{f.message}" }.join(', ')
              logger.warn("Some messages failed to delete: #{failed}")
            else
              logger.info("Successfully deleted #{delete_response.successful.size} messages from DLQ")
            end
          rescue Aws::SQS::Errors::ServiceError => e
            logger.error("Failed to delete messages from DLQ: #{e.message}")
          end
        end
      end
      
      # Show progress
      if options[:max_messages]
        progress = (stats[:messages_sent].to_f / options[:max_messages] * 100).round(2)
        logger.info("Progress: #{stats[:messages_sent]}/#{options[:max_messages]} messages processed (#{progress}%)")
      else
        logger.info("Total processed so far: #{stats[:messages_sent]} messages")
      end
      
    rescue Aws::SQS::Errors::ServiceError => e
      logger.error("SQS service error: #{e.message}")
      sleep 5 # Add delay before retrying
    rescue StandardError => e
      logger.error("Unexpected error: #{e.message}")
      logger.error(e.backtrace.join("\n"))
      sleep 5 # Add delay before retrying
    end
  end
  
  # Log final statistics
  logger.info("DLQ retry process completed")
  logger.info("Total statistics:")
  logger.info("- Messages received from DLQ: #{stats[:messages_received]}")
  logger.info("- Messages successfully sent to destination: #{stats[:messages_sent]}")
  logger.info("- Messages failed to process: #{stats[:messages_failed]}")
  logger.info("- Batches processed: #{stats[:batches_processed]}")
  
rescue StandardError => e
  logger.error("Initialization error: #{e.message}")
  logger.error(e.backtrace.join("\n"))
  exit(1)
end

logger.info("DLQ retry processor shutdown complete")

