class ConnectionManager
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
      ip = addr[3]
      port = addr[1]
      key = [ip, port]
      type_id = data.ord
      msg = data[1..]
      p [:recv, Connection::TYPES[type_id], msg]
      case Connection::TYPES[type_id]
      when :req
        find_or_accept_connection(ip, port)&.send_ack
      when :ack
        rid, ridx, sid, scnt = msg.unpack 'NNNN'
        @connections[key].handle_ack rid, ridx, sid, scnt
      when :data
        connection_id, idx = msg.unpack 'NN'
        @connections[key]&.handle_data connection_id, idx, msg[8..]
      when :resend
        connection_id, *idxs = msg.unpack 'N*'
        @connections[key]&.handle_resend connection_id, idxs
      when :close
        @connections[key]&.handle_close
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

  def send_raw(data, ip, port)
    p [:raw, data]
    @socket.send data, 0, ip, port
  end

  def accept
    @accept_queue.deq
  end

  def connect(ip, port)
    conn = @connections[[ip, port]]
    unless conn
      conn = @connections[[ip, port]] = Connection.new self, ip, port
      conn.send_req
    end
    conn
  end

  def run_tick
    loop do
      @connections.each_value { _1.tick }
      sleep 1
    end
  end

  def remove(ip, port)
    @connections.delete [ip, port]
  end
end

class Connection
  TYPES = %i[req ack data resend close]

  def initialize(manager, ip, port)
    @manager = manager
    @send_connection_id = rand 0xffffffff
    @ip = ip
    @port = port
    @event_queue = Thread::Queue.new
    @status = :connecting
    @initialized_at = @last_recv_ack = Time.now
    @last_send_ack = nil
    @terminated_recv_connection_id = nil
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
    trigger :close
    @manager.remove @ip, @port
  end

  def trigger(type, message = nil)
    @event_queue << [type, message]
  end

  def read
    @event_queue.deq
  end

  def send_data(message)
    message.chars.each_slice(1024).map(&:join).map do |msg|
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
      send_ack if @last_send_ack.nil? || current - @last_send_ack > 5
    end
  end

  def send_ack
    @last_send_ack = Time.now
    socket_send :ack, @recv_connection_id, @recv_idx, @send_connection_id, @send_idx + @send_buffer.size
  end

  def handle_ack(send_id, reached_idx, recv_id, remote_sent_count)
    if connecting?
      trigger :open
      @status = :connected
    end
    return if send_id != @send_connection_id && reached_idx != 0
    return if recv_id == @terminated_recv_connection_id
    reset_recv_connection recv_id if recv_id != @recv_connection_id
    if reached_idx >= @send_idx
      @send_buffer.shift reached_idx - @send_idx
    else
      reset_send_connection
      trigger :reset
    end
    @send_idx = reached_idx
    i = remote_sent_count - 1 - @recv_idx
    @recv_buffer[i] ||= nil if i >= 0
    @last_recv_ack = Time.now
    request_resend
  end

  def handle_data(recv_id, idx, data)
    reset_recv_connection recv_id if recv_id != @recv_connection_id
    @recv_buffer[idx - @recv_idx] = data
    @unreceiveds.delete idx
    while @recv_buffer.first
      @recv_idx += 1
      trigger :data, @recv_buffer.shift
    end
    request_resend
  end

  def handle_resend(send_id, idxs)
    return if send_id != @send_connection_id
    idxs.each do |idx|
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
    timeout = 200
    ids = @unreceiveds.keys.select do |id|
      current - @unreceiveds[id] > timeout
    end
    return if ids.empty?
    ids.each { @unreceiveds[_1] = current + timeout }
    socket_send :resend, @recv_connection_id, ids
  end

  def socket_send(type, *data)
    p [:socket_send, type, data]
    sdata = data.flatten.map { _1.is_a?(Numeric) ? [_1].pack('N') : _1 }.join
    type_id = TYPES.index type
    @manager.send_raw type_id.chr + sdata, @ip, @port
  end
end