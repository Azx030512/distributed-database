const readline = require('readline');
const axios = require('axios');
const net = require('net');
let cache = {};

let rl = readline.createInterface({ 
  input: process.stdin,
  output: process.stdout,
  prompt: 'ClientShell> '
});

function sendRequestToMaster(jsonData, url) {
  return axios.post(url, jsonData).then(response => {
    if (response.status == 200)
        return response.data;
    else
        console.log('Error: Failed to connect to Master.');
  }).catch(e => console.log(e));
}

function sendJsonToRegionServer(data, [host, port]) {
  let client = new net.Socket();
  client.connect(port, host, function() {
    client.write(data);
  });

  client.on('data', function(response) {
    let response_data = JSON.parse(response);
    switch(response_data.success) {
      case 0:
        console.log("Operation failed: " + response_data.message);
        break;
      case 1:
        console.log("Create operation succeeded: " + response_data.message);
        break;
      case 2:
        console.log("Drop operation succeeded: " + response_data.message);
        break;
      case 3:
        console.log("Write operation succeeded: " + response_data.message);
        break;
      case 4:
        console.log("Read operation succeeded: " + response_data.message);
        console.log("Data: " + JSON.stringify(response_data.data));
        break;
    }
  });

  client.on('end', function() {
    client.destroy();
  });
}

//  开始显示欢迎信息
console.log('Welcome to JavaScript Client Shell. Type "help" for usage.');
rl.on('line', (input) => {
  let inputParts = input.split(' ');
  let command = inputParts.shift();
  let args = inputParts.join(' ');
  let data = null;
  switch(command) {
    case 'create':
        var estimated_size = args.split(' ')[0];
        var sql_statement = args.split(' ').slice(1).join(' ');
        if (!estimated_size || !sql_statement) {
            console.log('Invalid argument. Usage: create <estimated_size> <sql_statement>');
            break;
        }
        if (isNaN(estimated_size) || estimated_size < 0) {
            console.log('Invalid estimated size. Please use a positive number.');
            break;
        }
        operation = sql_statement.split(' ')[0].toUpperCase();
        if (operation != 'CREATE') {
            console.log('Invalid operation. Please use CREATE with create.');
            break;
        }
        var table_name = sql_statement.split(' ')[2];
        data = {table_name: table_name, estimated_size: estimated_size};
        sendRequestToMaster(data, 'http://127.0.0.1:5000/api/route/create_table').then(response => {
            if (response.signal === 'success') {
                cache[table_name] = response.addresses;
                response.addresses.forEach(item => {
                    sendJsonToRegionServer(JSON.stringify({"1": sql_statement}), item.split(':'));
                });
            }
        });
        break;
    case 'read':
      if (args.length != 1) {
          // console.log(args.length)
          console.log('Invalid argument. Usage: read <sql_statement>');
          break;
      }
      var sql_statement = args[0];
      // operation转为大写
      operation = args[0].split(' ')[0].toUpperCase();
      if (operation != 'SELECT') {
          console.log('Invalid operation. Please use SELECT with read.');
          break;
      }
      var table_name = args[0].split(' ')[3];
      if (table_name in cache) {
          sendJsonToRegionServer(JSON.stringify({"4": sql_statement}), cache[table_name][0].split(':'));
      } else {
          data = {table_name: table_name};
          sendRequestToMaster(data, 'http://127.0.0.1:5000/api/route/read_table').then(response => {
              if (response.signal === 'success') {
                  cache[table_name] = [response.address];
                  sendJsonToRegionServer(JSON.stringify({"4": sql_statement}), response.address.split(':'));
              }
          });
      }
      break;
    case 'write':
      if (args.length != 1) {
        console.log('Invalid argument. Usage: write <sql_statement>');
        break;
      }
      var table_name = args[0].split(' ')[2];
      operation = args[0].split(' ')[0].toUpperCase();
      if (operation != 'INSERT' && operation != 'UPDATE' && operation != 'DELETE') {
        console.log('Invalid operation. Please use INSERT, UPDATE or DELETE with write.');
        break;
      }
      if (table_name in cache) {
        cache[table_name].forEach(item => {
          sendJsonToRegionServer(JSON.stringify({"3": args[0]}), item.split(':'));
        });
      } else {
        data = {table_name: table_name};
        sendRequestToMaster(data, 'http://127.0.0.1:5000/api/route/write_table').then(response => {
          if (response.signal === 'success') {
            cache[table_name] = response.addresses;
            response.addresses.forEach(item => {
              sendJsonToRegionServer(JSON.stringify({"3": args[0]}), item.split(':'));
            });
          }
        });
      }
      break;
    case 'drop':
      if (args.length != 1) {
        console.log('Invalid argument. Usage: drop <table_name>');
        break;
      }
      sql_statement = `DROP TABLE ${args[0]}`;
      data = {table_name: args[0]};
      sendRequestToMaster(data, 'http://127.0.0.1:5000/api/route/drop_table').then(response => {
        if (response.signal === 'success') {
          if (args[0] in cache) {
            delete cache[args[0]];
          }
          response.addresses.forEach(item => {
            sendJsonToRegionServer(JSON.stringify({"2": sql_statement}), item.split(':'));
          });
        }
      });
      break;
    case 'quit':
      rl.close();
      process.exit(0);
    case 'help':
      console.log('Commands:');
      console.log('create <estimated_size> <sql_statement> - Create a table with estimated size and SQL statement.');
      console.log('read <sql_statement> - Read data from a table with SQL statement.');
      console.log('write <sql_statement> - Write data to a table with SQL statement.');
      console.log('drop <table_name> - Drop a table with table name.');
      console.log('quit - Exit the shell.');
      break;
    default:
      console.log(`Unrecognized command: ${command}`);
  }
  rl.prompt();
});

rl.prompt();