## Usage:

```
Usage: <main class> [command] [command options]
  Commands:
    server      Start an arithmetic server
      Usage: server [options]
        Options:
        * --id, -i
            Raft id of this server
        * --peers, -r
            Raft peers (format: name:host:port,name:host:port)
          --raftGroup, -g
            Raft group identifier
            Default: demoRaftGroup123
        * --storage, -s
            Storage dir

    assign      Assign value to a variable.
      Usage: assign [options]
        Options:
        * --name
            Name of the variable to set
        * --peers, -r
            Raft peers (format: name:host:port,name:host:port)
          --raftGroup, -g
            Raft group identifier
            Default: demoRaftGroup123
        * --value
            Value to set

    get      Assign value to a variable.
      Usage: get [options]
        Options:
        * --name
            Name of the variable to set
        * --peers, -r
            Raft peers (format: name:host:port,name:host:port)
          --raftGroup, -g
            Raft group identifier
            Default: demoRaftGroup123
```

## Run examples:

####To run a single ratis server

```
java -jar ratis-examples-0.1.1-alpha-SNAPSHOT.jar org.apache.ratis.examples.arithmetic.Runner server -i n1 -s target/n1 -r n1:127.0.0.1:8882
```

####To run a single ratis client

```
# Assign x=10
java -jar ratis-examples-0.1.1-alpha-SNAPSHOT.jar org.apache.ratis.examples.arithmetic.Runner assign -r n1:127.0.0.1:8882 --name x --value 10
# Assign y=x+2
java -jar ratis-examples-0.1.1-alpha-SNAPSHOT.jar org.apache.ratis.examples.arithmetic.Runner assign -r n1:127.0.0.1:8882 --name y --value x+2

# Get x should print 10
java -jar ratis-examples-0.1.1-alpha-SNAPSHOT.jar org.apache.ratis.examples.arithmetic.Runner get -r n1:127.0.0.1:8882 --name x
# Get y should print 12
java -jar ratis-examples-0.1.1-alpha-SNAPSHOT.jar org.apache.ratis.examples.arithmetic.Runner get -r n1:127.0.0.1:8882 --name y
```