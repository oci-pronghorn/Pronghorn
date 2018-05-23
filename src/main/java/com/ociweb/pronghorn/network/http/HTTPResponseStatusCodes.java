package com.ociweb.pronghorn.network.http;

public class HTTPResponseStatusCodes {

    /* Source: https://en.wikipedia.org/wiki/List_of_HTTP_status_codes */

    public static byte[] codes[] = statusCodeTable();
    
    public static byte[][] statusCodeTable() {
    	byte[][] result = new byte[600][];
    	
    	result[100] = " 100 Continue\r\n".getBytes();
    	result[101] = " 101 Switching Protocols\r\n".getBytes();
    	result[102] = " 102 Processing\r\n".getBytes();
    	result[103] = " 103 Early Hints\r\n".getBytes();
    	result[200] = " 200 OK\r\n".getBytes();
    	result[201] = " 201 Created\r\n".getBytes();
    	result[202] = " 202 Accepted\r\n".getBytes();
    	result[203] = " 203 Non-Authoritative Information\r\n".getBytes();
    	result[204] = " 204 No Content\r\n".getBytes();
    	result[205] = " 205 Reset Content\r\n".getBytes();
    	result[206] = " 206 Partial Content\r\n".getBytes();
    	result[207] = " 207 Multi-Status\r\n".getBytes();
    	result[208] = " 208 Already Reported\r\n".getBytes();
    	result[226] = " 226 IM Used\r\n".getBytes();
    	result[300] = " 300 Multiple Choices\r\n".getBytes();
    	result[301] = " 301 Moved Permanently\r\n".getBytes();
    	result[302] = " 302 Found\r\n".getBytes();
    	result[303] = " 303 See Other\r\n".getBytes();
    	result[304] = " 304 Not Modified\r\n".getBytes();
    	result[305] = " 305 Use Proxy\r\n".getBytes();
    	result[306] = " 306 Switch Proxy\r\n".getBytes();
    	result[307] = " 307 Temporary Redirect\r\n".getBytes();
    	result[308] = " 308 Permanent Redirect\r\n".getBytes();
    	result[400] = " 400 Bad Request\r\n".getBytes();
    	result[401] = " 401 Unauthorized\r\n".getBytes();
    	result[402] = " 402 Payment Required\r\n".getBytes();
    	result[403] = " 403 Forbidden\r\n".getBytes();
    	result[404] = " 404 Not Found\r\n".getBytes();
    	result[405] = " 405 Method Not Allowed\r\n".getBytes();
    	result[407] = " 407 Proxy Authentication Required\r\n".getBytes();
    	result[408] = " 408 Request Timeout\r\n".getBytes();
    	result[409] = " 409 Conflict\r\n".getBytes();
    	result[410] = " 410 Gone\r\n".getBytes();
    	result[411] = " 411 Length Required\r\n".getBytes();
    	result[412] = " 412 Precondition Failed\r\n".getBytes();
    	result[413] = " 413 Payload Too Large\r\n".getBytes();
    	result[414] = " 414 URI Too Long\r\n".getBytes();
    	result[415] = " 415 Unsupported Media Type\r\n".getBytes();
    	result[416] = " 416 Range Not Satisfiable\r\n".getBytes();
    	result[417] = " 417 Expectation Failed\r\n".getBytes();
    	result[418] = " 418 I'm a teapot\r\n".getBytes(); /* essential */
    	result[421] = " 421 Misdirected Request\r\n".getBytes();
    	result[422] = " 422 Unprocessable Entity\r\n".getBytes();
    	result[423] = " 423 Locked\r\n".getBytes();
    	result[424] = " 424 Failed Dependency\r\n".getBytes();
    	result[426] = " 426 Upgrade Required\r\n".getBytes();
    	result[428] = " 428 Precondition Required\r\n".getBytes();
    	result[429] = " 429 Too Many Requests\r\n".getBytes();
    	result[431] = " 431 Request Header Fields Too Large\r\n".getBytes();
    	result[451] = " 451 Unavailable For Legal Reasons\r\n".getBytes();
    	result[500] = " 500 Internal Server Error\r\n".getBytes();
    	result[501] = " 501 Not Implemented\r\n".getBytes();
    	result[502] = " 502 Bad Gateway\r\n".getBytes();
    	result[503] = " 503 Service Unavailable\r\n".getBytes();
    	result[504] = " 504 Gateway Timeout\r\n".getBytes();
    	result[505] = " 505 HTTP Version Not Supported\r\n".getBytes();
    	result[506] = " 506 Variant Also Negotiates\r\n".getBytes();
    	result[508] = " 508 Loop Detected\r\n".getBytes();
    	result[510] = " 510 Not Extended\r\n".getBytes();
    	result[511] = " 511 Network Authentication Required\r\n".getBytes();
    	
    	return result;
    }

}
