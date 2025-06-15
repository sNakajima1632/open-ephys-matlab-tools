classdef OpenEphysStreamer < handle
    properties (Constant)
        CHANNEL_SHOWN = 0;
    end

    properties
        context
        dataSocket
        eventSocket
        poller
        messageNum = -1;
        socketWaitsReply = false;
        eventNo = 0;
        appName = 'TxBDC PCMS';
        uuidStr
        lastHeartbeatTime = 0;
        lastReplyTime
    end

    methods
        function obj = OpenEphysStreamer()
            obj.context = zmq.core.ctx_new();
            obj.dataSocket = [];
            obj.eventSocket = [];
            obj.poller = zmq.core.poller_new();
            obj.uuidStr = char(java.util.UUID.randomUUID());
            obj.lastReplyTime = tic;
        end

        function initialize(obj)
            if isempty(obj.dataSocket)
                obj.dataSocket = zmq.core.socket(obj.context, 'ZMQ_SUB');
                zmq.core.connect(obj.dataSocket, 'tcp://localhost:5556');
                zmq.core.setsockopt(obj.dataSocket, 'ZMQ_SUBSCRIBE', '');

                obj.eventSocket = zmq.core.socket(obj.context, 'ZMQ_REQ');
                zmq.core.connect(obj.eventSocket, 'tcp://localhost:5557');

                zmq.core.poller_add(obj.poller, obj.dataSocket, ZMQ_POLLIN);
                zmq.core.poller_add(obj.poller, obj.eventSocket, ZMQ_POLLIN);
            end
        end

        function sendHeartbeat(obj)
            msg = jsonencode(struct( ...
                'application', obj.appName, ...
                'uuid', obj.uuidStr, ...
                'type', 'heartbeat'));
            zmq.core.send(obj.eventSocket, msg, 0);
            obj.lastHeartbeatTime = toc;
            obj.socketWaitsReply = true;
        end

        function [dataOut, sr] = callback(obj)
            dataOut = [];
            sr = [];

            % Heartbeat timer
            if toc - obj.lastHeartbeatTime > 2
                if obj.socketWaitsReply
                    if toc - obj.lastReplyTime > 10
                        zmq.core.poller_remove(obj.poller, obj.eventSocket);
                        zmq.core.close(obj.eventSocket);
                        obj.eventSocket = zmq.core.socket(obj.context, 'ZMQ_REQ');
                        zmq.core.connect(obj.eventSocket, 'tcp://localhost:5557');
                        zmq.core.poller_add(obj.poller, obj.eventSocket, ZMQ_POLLIN);
                        obj.socketWaitsReply = false;
                        obj.lastReplyTime = tic;
                    end
                else
                    obj.sendHeartbeat();
                end
            end

            % Poll
            socks = zmq.core.poll(obj.poller, 1);
            if ~isempty(socks)
                if socks == obj.dataSocket
                    msg = zmq.core.recv(obj.dataSocket, 'ZMQ_DONTWAIT|ZMQ_RCVMORE');
                    % Assume 2nd part is JSON header, 3rd part is binary frames
                    [~, more] = zmq.core.getsockopt(obj.dataSocket, 'ZMQ_RCVMORE');
                    hdrRaw = zmq.core.recv(obj.dataSocket, 0);
                    hdr = jsondecode(char(hdrRaw'));

                    if strcmp(hdr.type, 'data')
                        n = hdr.content.num_samples;
                        ch = hdr.content.channel_num;
                        sr = hdr.content.sample_rate;
                        if ch == obj.CHANNEL_SHOWN
                            binData = zmq.core.recv(obj.dataSocket, 0);
                            floats = typecast(uint8(binData), 'single');
                            dataOut = reshape(floats, [1, n]);
                        end
                    end
                elseif socks == obj.eventSocket && obj.socketWaitsReply
                    _ = zmq.core.recv(obj.eventSocket, 0);
                    obj.socketWaitsReply = false;
                end
            end
        end
    end
end
