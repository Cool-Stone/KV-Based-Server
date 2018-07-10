# KV-Based-Server

### 1. **Compile**

    $ cd ~/Downloads/KV-Based-Server
    $ make
    

### 2. **Usage**

    $ ./server <port> // if port is not given, default port is 9000

In another terminal
    
    $ ./client <address> <port>

for example

    $ ./server 9000
    $ ./client 127.0.0.1 9000   // ./client localhost 9000 is also ok


### 3. **Unit test & Press test**

    $ ./utest < debug / ui / con >
    $ ./press 127.0.0.1 9000 < set / get / del >

### 4. **Result**

  For 1000 users, 100 requests:

  * set  30ms

  * get  17ms

  * del  19ms

  For 10000 users, 100 requests:

  * set 3.1ms

  * get 1.7ms

  * del 1.9ms
