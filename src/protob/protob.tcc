#include "protob/protob.hpp"

#include <google/protobuf/stubs/common.h>

#include <string>

#include "errors.hpp"
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>

#include "arch/arch.hpp"

template <class request_t, class response_t, class context_t>
protob_server_t<request_t, response_t, context_t>::protob_server_t(int port, int http_port, boost::function<response_t(request_t *, context_t *)> _f, response_t (*_on_unparsable_query)(request_t *), protob_server_callback_mode_t _cb_mode)  // NOLINT(readability/casting)
    : next_http_conn_id(0), f(_f), on_unparsable_query(_on_unparsable_query), cb_mode(_cb_mode) {
    tcp_listener.init(new tcp_listener_t(port, boost::bind(&protob_server_t<request_t, response_t, context_t>::handle_conn, this, _1, auto_drainer_t::lock_t(&auto_drainer))));
    http_server.init(new http_server_t(http_port, this));
}

template <class request_t, class response_t, class context_t>
protob_server_t<request_t, response_t, context_t>::~protob_server_t() { }

template <class request_t, class response_t, class context_t>
void protob_server_t<request_t, response_t, context_t>::handle_conn(const scoped_ptr_t<nascent_tcp_conn_t> &nconn, auto_drainer_t::lock_t keepalive) {
    context_t ctx;
    scoped_ptr_t<tcp_conn_t> conn;
    nconn->ennervate(&conn);

    //TODO figure out how to do this with less copying
    for (;;) {
        request_t request;
        bool force_response = false;
        response_t forced_response;
        try {
            int32_t size;
            conn->read(&size, sizeof(int32_t), keepalive.get_drain_signal());

            scoped_array_t<char> data(size);
            conn->read(data.data(), size, keepalive.get_drain_signal());

            bool res = request.ParseFromArray(data.data(), size);
            if (!res) {
                forced_response = (*on_unparsable_query)(&request);
                force_response = true;
            }
        } catch (tcp_conn_read_closed_exc_t &) {
            //TODO need to figure out what blocks us up here in non inline cb
            //mode
            return;
        }

        try {
            switch (cb_mode) {
                case INLINE:
                    if (force_response) {
                        send(forced_response, conn.get(), keepalive.get_drain_signal());
                    } else {
                        send(f(&request, &ctx), conn.get(), keepalive.get_drain_signal());
                    }
                    break;
                case CORO_ORDERED:
                    crash("unimplemented");
                    break;
                case CORO_UNORDERED:
                    crash("unimplemented");
                    break;
                default:
                    crash("unreachable");
                    break;
            }
        } catch (tcp_conn_write_closed_exc_t &) {
            //TODO need to figure out what blocks us up here in non inline cb
            //mode
            return;
        }
    }
}

template <class request_t, class response_t, class context_t>
void protob_server_t<request_t, response_t, context_t>::send(const response_t &res, tcp_conn_t *conn, signal_t *closer) THROWS_ONLY(tcp_conn_write_closed_exc_t) {
    int size = res.ByteSize();
    conn->write(&size, sizeof(res.ByteSize()), closer);
    scoped_array_t<char> data(size);

    res.SerializeToArray(data.data(), size);
    conn->write(data.data(), size, closer);
}

template <class request_t, class response_t, class context_t>
http_res_t protob_server_t<request_t, response_t, context_t>::handle(const http_req_t &req) {
    if (req.method == POST &&
        req.resource.as_string().find("close-connection") != std::string::npos) {

        boost::optional<std::string> optional_conn_id = req.find_query_param("conn_id");
        std::string string_conn_id = *optional_conn_id;
        int32_t conn_id = boost::lexical_cast<int32_t>(string_conn_id);
        http_conns.erase(conn_id);

        http_res_t res(HTTP_OK);
        res.version = "HTTP/1.1";
        res.add_header_line("Access-Control-Allow-Origin", "*");

        return res;
    } else if (req.method == GET && req.resource.as_string() == "/open-new-connection") {

        int32_t conn_id = ++next_http_conn_id;
        http_conns.insert(std::pair<int32_t,context_t>(++conn_id,context_t()));

        http_res_t res(HTTP_OK);
        res.version = "HTTP/1.1";
        res.add_header_line("Access-Control-Allow-Origin", "*");
        std::string body_data;
        body_data.assign(((char *)&conn_id), sizeof(conn_id));
        res.set_body("application/octet-stream", body_data);

        return res;
    } else {

        boost::optional<std::string> optional_conn_id = req.find_query_param("conn_id");
        std::string string_conn_id = *optional_conn_id;
        int32_t conn_id = boost::lexical_cast<int32_t>(string_conn_id);

        // Extract protobuf from http request body
        const char *data = req.body.data();
        int32_t req_size = *reinterpret_cast<const int32_t *>(data);
        data += sizeof(req_size);

        request_t request;
        bool parseSucceeded = request.ParseFromArray(data, req_size);

        typename std::map<int32_t, context_t>::iterator it;
        response_t response;
        switch(cb_mode) {
        case INLINE:
            it = http_conns.find(conn_id);
            if (!parseSucceeded || it == http_conns.end()) {
                response = on_unparsable_query(&request);
            } else {
                context_t *ctx = &(it->second);
                response = f(&request, ctx);
            }
            break;
        case CORO_ORDERED:
        case CORO_UNORDERED:
            crash("unimplemented");
        default:
            crash("unreachable");
            break;
        }

        int32_t res_size = response.ByteSize();
        scoped_array_t<char> res_data(sizeof(res_size) + res_size);
        *reinterpret_cast<int32_t *>(res_data.data()) = res_size;
        response.SerializeToArray(res_data.data() + sizeof(res_size), res_size);

        http_res_t res(HTTP_OK);
        res.version = "HTTP/1.1";
        res.add_header_line("Access-Control-Allow-Origin", "*");

        std::string body_data;
        body_data.assign(res_data.data(), res_size + sizeof(res_size));
        res.set_body("application/octet-stream", body_data);

        return res;
    }
}
