/**
 * Created by Administrator on 2018/4/20.
 */
console.log("hello");

var http=require('http');

http.createServer(function ( request,response) {
    response.writeHead(200,{'Content-type':'text/plain'});
    response.end("hello\n")
    
}).listen(8888)

console.log('server is running http://127.0.0.1:8888')