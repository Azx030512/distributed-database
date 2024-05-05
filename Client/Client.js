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
    client.end();
  });
}

//  开始显示欢迎信息
console.log('Welcome to JavaScript Client Shell. Type "help" for usage.');
rl.on('line', (input) => {
  let [command, ...args] = input.split(' ');
  let data = null;
  switch(command) {
    case 'create':
      if (args.length != 2) {
        console.log('Invalid argument. Usage: create <table_name> <estimated_size>');
        break;
      }
      data = {table_name: args[0], estimated_size: args[1]};
      sendRequestToMaster(data, 'http://127.0.0.1:5000/api/route/create_table').then(response => {
        if (response.signal === 'success') {
          cache[args[0]] = response.addresses;
          response.addresses.forEach(item => {
            sendJsonToRegionServer(JSON.stringify({"1": args[0]}), item.split(':'));
          });
        }
      });
      break;
    case 'read':
      if (args.length != 1) {
        console.log('Invalid argument. Usage: read <table_name>');
        break;
      }
      if (args[0] in cache) {
        sendJsonToRegionServer(JSON.stringify({"4": args[0]}), cache[args[0]][0].split(':'));
      } else {
        data = {table_name: args[0]};
        sendRequestToMaster(data, 'http://127.0.0.1:5000/api/route/read_table').then(response => {
          if (response.signal === 'success') {
            cache[args[0]] = [response.address];
            sendJsonToRegionServer(JSON.stringify({"4": args[0]}), response.address.split(':'));
          }
        });
      }
      break;
    case 'write':
      if (args.length != 2) {
        console.log('Invalid argument. Usage: write <table_name> <new_data>');
        break;
      }
      if (args[0] in cache) {
        cache[args[0]].forEach(item => {
          sendJsonToRegionServer(JSON.stringify({"3": {[args[0]]: args[1]}}), item.split(':'));
        });
      } else {
        data = {table_name: args[0]};
        sendRequestToMaster(data, 'http://127.0.0.1:5000/api/route/write_table').then(response => {
          if (response.signal === 'success') {
            cache[args[0]] = response.addresses;
            response.addresses.forEach(item => {
              sendJsonToRegionServer(JSON.stringify({"3": {[args[0]]: args[1]}}), item.split(':'));
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
      data = {table_name: args[0]};
      sendRequestToMaster(data, 'http://127.0.0.1:5000/api/route/drop_table').then(response => {
        if (response.signal === 'success') {
          if (args[0] in cache) {
            delete cache[args[0]];
          }
          response.addresses.forEach(item => {
            sendJsonToRegionServer(JSON.stringify({"2": args[0]}), item.split(':'));
          });
        }
      });
      break;
    case 'quit':
      rl.close();
      process.exit(0);
    case 'help':
      console.log(`
        Usage:
        create <table_name> <estimated_size>   Create a new table.
        read <table_name>                      Read a table.
        write <table_name> <new_data>          Write to a table.
        drop <table_name>                      Drop a table.
        quit                                   Quit the shell.
        `);
      break;
    default:
      console.log(`Unrecognized command: ${command}`);
  }
  rl.prompt();
});

rl.prompt();