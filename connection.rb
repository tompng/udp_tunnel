require 'monitor'

class ConnectionManager
  attr_accessor :on_connect, :on_close, :on_unhandled_data, :socket
  def initialize(socket, accept: true)
    @socket = socket
    @connections = {}
    @accept_queue = Thread::Queue.new
    @accept_new_connection = accept
    Thread.new { run_recv }
    Thread.new { run_tick }
  end

  def accept_new_connection? = @accept_new_connection

  def run_recv
    loop do
      data, addr = @socket.recvfrom 65536
      unless data[0, 3] == Connection::SIGNATURE
        on_unhandled_data&.call data, addr
        next
      end
      ip = addr[3]
      port = addr[1]
      key = [ip, port]
      type_id = data[3].ord
      msg = data[4..]
      existing_conn = @connections[key]
      if existing_conn&.marked_for_accept
        existing_conn.marked_for_accept = false
        @accept_queue << existing_conn
      end
      case Connection::TYPES[type_id]
      when :req
        find_or_accept_connection(ip, port)&.send_ack
      when :ack
        rid, ridx, sid, scnt = msg.unpack 'NNNN'
        existing_conn&.handle_ack rid, ridx, sid, scnt
      when :data
        connection_id, idx = msg.unpack 'NN'
        existing_conn&.handle_data connection_id, idx, msg[8..]
      when :resend
        connection_id, *idxs = msg.unpack 'N*'
        existing_conn&.handle_resend connection_id, idxs
      when :close
        existing_conn&.handle_close
        @connections.delete key
      end
    end
  end

  def find_or_accept_connection(ip, port)
    key = [ip, port]
    conn = @connections[key]
    if conn.nil? && accept_new_connection?
      conn = @connections[key] = Connection.new self, ip, port
      @accept_queue << conn
    end
    conn
  end

  attr_accessor :emulate_packet_loss, :emulate_packet_delay

  def send_raw(data, ip, port)
    if emulate_packet_loss && rand < emulate_packet_loss
      p :loss
      return
    end
    if emulate_packet_delay
      Thread.new do
        sleep rand(emulate_packet_delay)
        @socket.send data, 0, ip, port
      end
    else
      @socket.send data, 0, ip, port
    end
  end

  def accept
    @accept_queue.deq
  end

  def connect(ip, port, mark_for_accept: false)
    conn = @connections[[ip, port]]
    unless conn
      conn = @connections[[ip, port]] = Connection.new self, ip, port
      conn.marked_for_accept = mark_for_accept
      conn.send_req
    end
    conn
  end

  def run_tick
    loop do
      @connections.each_value { _1.tick }
      sleep 0.1
    end
  end

  def remove(ip, port)
    @connections.delete [ip, port]
  end
end

class Connection
  TYPES = %i[req ack data resend close]
  SIGNATURE = [0x35, 0x26, 0x41].pack 'C*'

  attr_accessor :marked_for_accept
  attr_reader :ip, :port

  def initialize(manager, ip, port)
    @manager = manager
    @send_connection_id = rand 0xffffffff
    @ip = ip
    @port = port
    @event_queue = Thread::Queue.new
    @status = :connecting
    @initialized_at = @last_recv_ack = Time.now
    @last_send_ack = nil
    @last_send_ack_idx = 0
    @terminated_recv_connection_id = nil
    @monitor = Monitor.new
    @cond = @monitor.new_cond
    reset_send_connection
    reset_recv_connection 0
  end

  def reset_send_connection
    @send_buffer = []
    @send_idx = 0
  end

  def reset_recv_connection(recv_id)
    @terminated_recv_connection_id = @recv_connection_id
    @recv_connection_id = recv_id
    @recv_idx = 0
    @recv_buffer = []
    @unreceiveds = {}
  end

  def close
    return if closed?
    handle_close
    socket_send :close
  end

  def handle_close
    return if closed?
    @status = :closed
    @manager.on_close&.call self
    trigger :close
    @manager.remove @ip, @port
  end

  def trigger(type, message = nil)
    @event_queue << [type, message]
  end

  def read
    @event_queue.deq
  end

  MAX_BODY_SIZE = 1024
  MAX_PACKET_BUFFER = 128

  def send_data(message)
    @monitor.synchronize do
      @cond.wait_while { @send_buffer.size >= MAX_PACKET_BUFFER }
    end
    message.chars.each_slice(MAX_BODY_SIZE).map(&:join).map do |msg|
      idx = @send_idx + @send_buffer.size
      @send_buffer << msg
      socket_send :data, @send_connection_id, idx, msg
    end
  end

  def closed? = @status == :closed
  def connecting? = @status == :connecting
  def connected? = @status == :connected

  def send_close
    return if closed?
    socket_send :close
  end

  def send_req
    @last_send_req = Time.now
    socket_send :req
  end

  def tick
    current = Time.now
    if connecting?
      if current - @initialized_at > 30
        handle_close
      elsif @last_send_req.nil? || current - @last_send_req > 1
        send_req
      end
    elsif connected?
      return handle_close if current - @last_recv_ack > 30
      send_ack if @last_send_ack.nil? || current - @last_send_ack > 5
      request_resend
    end
  end

  def send_ack
    @last_send_ack = Time.now
    @last_send_ack_idx = @recv_idx
    socket_send :ack, @recv_connection_id, @recv_idx, @send_connection_id, @send_idx + @send_buffer.size
  end

  def handle_ack(send_id, reached_idx, recv_id, remote_sent_count)
    if connecting?
      trigger :open
      @status = :connected
      @manager.on_connect&.call self
    end
    return if send_id != @send_connection_id && reached_idx != 0
    return if recv_id == @terminated_recv_connection_id
    reset_recv_connection recv_id if recv_id != @recv_connection_id
    if reached_idx >= @send_idx
      @send_buffer.shift reached_idx - @send_idx
      @send_idx = reached_idx
    elsif reached_idx == 0
      reset_send_connection
      trigger :reset
    end
    i = remote_sent_count - 1 - @recv_idx
    @recv_buffer[i] ||= nil if i >= 0
    @last_recv_ack = Time.now
    request_resend
    @monitor.synchronize do
      @cond.signal if @send_buffer.size < MAX_PACKET_BUFFER
    end
  end

  def handle_data(recv_id, idx, data)
    reset_recv_connection recv_id if recv_id != @recv_connection_id && recv_id != @terminated_recv_connection_id
    @recv_buffer[idx - @recv_idx] = data if idx >= @recv_idx
    @unreceiveds.delete idx
    while @recv_buffer.first
      @recv_idx += 1
      trigger :data, @recv_buffer.shift
    end
    send_ack if @recv_idx > @last_send_ack_idx + 16
    request_resend
  end

  def handle_resend(send_id, idxs)
    return if send_id != @send_connection_id
    idxs.each do |idx|
      next if idx < @send_idx
      msg = @send_buffer[idx - @send_idx]
      socket_send :data, @send_connection_id, idx, msg
    end
  end

  def request_resend
    current = Time.now
    @recv_buffer.each.with_index @recv_idx do |msg, id|
      next if msg
      @unreceiveds[id] ||= current
    end
    timeout = 0.2
    max_resend_req = (MAX_BODY_SIZE - 5) / 4
    missing_ids = []
    @unreceiveds.each_key do |id|
      if current - @unreceiveds[id] > timeout
        missing_ids << id
        break if missing_ids.size >= max_resend_req
      end
    end
    return if missing_ids.empty?
    missing_ids.each { @unreceiveds[_1] = current + timeout }
    socket_send :resend, @recv_connection_id, missing_ids
  end

  def socket_send(type, *data)
    sdata = data.flatten.map { _1.is_a?(Numeric) ? [_1].pack('N') : _1 }.join
    type_id = TYPES.index type
    @manager.send_raw SIGNATURE + type_id.chr + sdata, @ip, @port
  end
end
