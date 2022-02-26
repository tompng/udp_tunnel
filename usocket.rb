require 'securerandom'
class USocket
  def initialize(connection, client, id)
    @client = client
    @connection = connection
    @id = id
    @read_queue = Thread::Queue.new
    @closed = false
  end

  def closed? = @closed

  def readpartial(*)
    return '' if closed?
    @read_queue.deq || ''
  end

  def handle_close
    return if closed?
    @closed = true
    @read_queue << nil
  end

  def handle_data(data)
    @read_queue << data
  end

  def close
    return if closed?
    @closed = true
    send_ctrl 'X'
    @client.remove @id
  end

  def send_ctrl(type, data = '')
    @connection.send_data type + @id + [data.size].pack('N') + data rescue exit
  end

  def write(data)
    return if closed?
    send_ctrl 'D', data unless data.nil? || data.empty?
  end

  def flush() = nil
end

class Client
  def initialize(connection, accept_queue = nil)
    @sockets = {}
    @accept_queue = accept_queue
    @connection = connection
    Thread.new { run_recv }
  end

  def run_recv
    rest = []
    loop do
      type, data = @connection.read
      case type
      when :data
        rest.push(*data.chars)
        while rest.size >= 9 && rest.size >= rest[5, 4].join.unpack1('N') + 9
          type = rest.shift
          id = rest.shift(4).join
          size = rest.shift(4).join.unpack1('N')
          handle_data id, type, rest.shift(size).join
        end
      when :reset, :close
        @sockets.each_value(&:handle_close)
        @sockets = {}
      end
      break if type == :close
    end
  end

  def handle_data(id, type, data)
    case type
    when 'O'
      return if @sockets[id] || @accept_queue.nil?
      socket = USocket.new @connection, self, id
      @sockets[id] = socket
      @accept_queue << socket
      socket.handle_data unless data.nil? || data.empty?
    when 'X'
      @sockets.delete(id)&.handle_close
    when 'D'
      @sockets[id]&.handle_data data
    end
  end

  def remove(id)
    @sockets.delete id
  end

  def connect
    id = SecureRandom.random_bytes 4
    socket = @sockets[id] = USocket.new @connection, self, id
    socket.send_ctrl 'O'
    socket
  end
end

class Server
  def initialize(manager)
    @manager = manager
    @accept_queue = Thread::Queue.new
    Thread.new { run_accept }
  end

  def run_accept
    loop do
      handle_connection @manager.accept
    end
  end

  def handle_connection(conn)
    Thread.new do
      Client.new conn, @accept_queue
    end
  end

  def accept
    @accept_queue.deq
  end
end
