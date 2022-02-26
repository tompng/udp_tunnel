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
      type = data.ord
      msg = data[1..]
      case type
      when Connection::TYPE_REQ
        find_or_accept_connection(ip, port, create: true)&.send_ack
      when Connection::TYPE_ACK
        rid, ridx, sid, scnt = msg.unpack 'NNN'
        find_or_accept_connection(ip, port, create: true)&.handle_ack rid, ridx, sid, scnt
      when Connection::TYPE_DATA
        connection_id, idx = msg.unpack 'NN'
        @connections[key]&.handle_data connection_id, idx, msg[4..]
      when Connection::TYPE_RESEND
        connection_id, idxs = msg.unpack 'N*'
        @connections[key]&.handle_resend connection_id, idxs
      when Connection::TYPE_CLOSE
        @connections[key]&.handle_close
        @connections.delete key
      end
    end
  end

  def find_or_accept_connection(ip, port)
    key = ip, port
    conn = @connections[key]
    if conn.nil? && accept_new_connection?
      conn = @connections[key] = Connection.new self, ip, port
      @accept_queue << conn
    end
    conn
  end

  def send_raw(data, ip, port)
    @socket.send data, 0, ip, port
  end

  def accept
    @accept_queue.deq
  end

  def connect(ip, port)
    conn = @connections[[ip, port]] ||= Connection.new socket, self, ip, port
    conn.send_req
  end

  def run_tick
    @connections.each_value { _1.tick }
  end
end

class Connection
  TYPE_REQ = 0
  TYPE_ACK = 1
  TYPE_DATA = 2
  TYPE_RESEND = 3

  def initialize(manager, ip, port)
    @manager = manager
    @send_connection_id = rand 0xffffffff
    @ip = ip
    @port = port
    @event_queue = Thread::Queue.new
    @state = :connecting
    @initialized_at = @last_recv_ack = Time.now
    @last_send_ack = nil
    @terminated_recv_connection_id = nil
    reset_send_connection
    reset_recv_connection nil
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
    socket_send TYPE_CLOSE
  end

  def handle_close
    return if closed?
    @state = :closed
    trigger :close
    @manager.delete [@ip, @port]
  end

  def trigger(type, message = nil)
    @event_queue << [type, message]
  end

  def read
    @event_queue.deq
  end

  def send_data(message)
    message.each_slice 1024 do |msg|
      idx = @send_idx + @send_buffer.size
      @send_buffer << msg
      socket_send TYPE_DATA, idx, msg
    end
  end

  def closed? = @state == :closed
  def connecting? = @state == :connecting
  def connected? = @state == :connected

  def send_close
    return if closed?
    socket_send TYPE_CLOSE
  end

  def send_req
    @last_send_req = Time.now
    socket_send TYPE_REQ
  end

  def tick
    current = Time.now
    if connecting?
      if @initialized_at - current > 30 * 1000
        handle_close
      elsif @last_send_req.nil? || @last_send_req - current > 1000
        send_req
      end
    elsif connected?
      send_ack if @last_send_ack - current > 5000
    end
  end

  def send_ack
    @last_send_ack = Time.now
    socket_send TYPE_ACK, @connection_id, @recv_idx, @send_idx + @send_buffer.size
  end

  def handle_ack(send_id, reached_idx, recv_id, remote_sent_count)
    if connecting?
      trigger :open
      @state = :connected
    end
    return if send_id != @send_connection_id
    return if recv_id == @terminated_recv_connection_id
    reset_recv_connection recv_id if recv_id != @recv_connection_id
    if reached_idx >= @send_idx
      @send_buffer.shift reached_idx - @send_idx
    else
      reset_send_connection
      trigger :reset
    end
    @send_idx = reached_idx
    @recv_buffer[remote_sent_count - 1 - @recv_idx] ||= nil if remote_sent_count > 0
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
      socket_send TYPE_DATA, @connection_id, idx, msg
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
    ids.each { @unreceiveds[_1] = current + timeout }
    socket_send TYPE_RESEND, @recv_connection_id, ids
  end

  def socket_send(type, *data)
    sdata = data.flatten.map { _1.is_a?(Number) ? [_1].pack('N') : _1 }.join
    @manager.send_raw type.chr + sdata, 0, @ip, @port
  end
end