classdef OpenEphysStreamer < handle
    
    properties (Constant)
        CHANNEL_SHOWN = 0;
    end

    properties
        context                         % = zmq.Context()
        dataSocket
        eventSocket
        poller                          % = zmq.Poller()
        messageNum = -1;
        socketWaitsReply = false;
        eventNo = 0;
        appName = 'TxBDC PCMS';
        uuidStr                         % = str(uuid.uuid4())
        lastHeartbeatTime = 0.0;
        lastReplyTime                   % = time.time()
    end

    methods

        function self = OpenEphysStreamer()
            % initialize OpenEphysStreamer object

            self.context = zmq.core.ctx_new();
            self.dataSocket = [];
            self.eventSocket = [];
            self.poller = zmq.core.poller_new();
            self.uuidStr = char(java.util.UUID.randomUUID());
            self.lastReplyTime = tic;
        end

        function initialize(self)

            % check to see if dataSocket has been defined
            if isempty(self.dataSocket)
                % initialize object variables
                self.dataSocket = zmq.core.socket(self.context, 'ZMQ_SUB');
                zmq.core.connect(self.dataSocket, 'tcp://localhost:5556');
                zmq.core.setsockopt(self.dataSocket, 'ZMQ_SUBSCRIBE', '');

                self.eventSocket = zmq.core.socket(self.context, 'ZMQ_REQ');
                zmq.core.connect(self.eventSocket, 'tcp://localhost:5557');

                zmq.core.poller_add(self.poller, self.dataSocket, ZMQ_POLLIN);
                zmq.core.poller_add(self.poller, self.eventSocket, ZMQ_POLLIN);
            end
        end

        function sendHeartbeat(self)
            msg = jsonencode(struct( ...
                'application', self.appName, ...
                'uuid', self.uuidStr, ...
                'type', 'heartbeat'));
            zmq.core.send(self.eventSocket, msg, 0);
            self.lastHeartbeatTime = toc;
            self.socketWaitsReply = true;
        end

        function [dataOut, sr] = callback(self)
            dataOut = [];
            sr = [];

            % Heartbeat timer
            if toc - self.lastHeartbeatTime > 2
                if self.socketWaitsReply
                    if toc - self.lastReplyTime > 10
                        zmq.core.poller_remove(self.poller, self.eventSocket);
                        zmq.core.close(self.eventSocket);
                        self.eventSocket = zmq.core.socket(self.context, 'ZMQ_REQ');
                        zmq.core.connect(self.eventSocket, 'tcp://localhost:5557');
                        zmq.core.poller_add(self.poller, self.eventSocket, ZMQ_POLLIN);
                        self.socketWaitsReply = false;
                        self.lastReplyTime = tic;
                    end
                else
                    self.sendHeartbeat();
                end
            end

            % Poll
            socks = zmq.core.poll(self.poller, 1);
            if ~isempty(socks)
                if socks == self.dataSocket
                    msg = zmq.core.recv(self.dataSocket, 'ZMQ_DONTWAIT|ZMQ_RCVMORE');
                    % Assume 2nd part is JSON header, 3rd part is binary frames
                    [~, more] = zmq.core.getsockopt(self.dataSocket, 'ZMQ_RCVMORE');
                    hdrRaw = zmq.core.recv(self.dataSocket, 0);
                    hdr = jsondecode(char(hdrRaw'));

                    if strcmp(hdr.type, 'data')
                        n = hdr.content.num_samples;
                        ch = hdr.content.channel_num;
                        sr = hdr.content.sample_rate;
                        if ch == self.CHANNEL_SHOWN
                            binData = zmq.core.recv(self.dataSocket, 0);
                            floats = typecast(uint8(binData), 'single');
                            dataOut = reshape(floats, [1, n]);
                        end
                    end
                elseif socks == self.eventSocket && self.socketWaitsReply
                    _ = zmq.core.recv(self.eventSocket, 0);
                    self.socketWaitsReply = false;
                end
            end
        end
    end
end
