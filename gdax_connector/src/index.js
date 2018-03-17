const GDAX = require('gdax');
const amqp = require('amqplib/callback_api');

console.log("Starting up GDAX connector...");

amqp.connect('amqp://messaging', registerExchangePerProduct);

function registerExchangePerProduct(err, message_conn) {

    if (err) {
        console.error(err);
        return;
    }

    getProducts(function(err, products) {

        message_conn.createChannel(function(err, channel) {
            for (var i = 0; i < products.length; i++) {
                var name = "GDAX-" + products[i];
                channel.assertExchange(name, 'fanout', {durable: false});
            }

            createWebsocketAndRun(products, channel);
        });
    });
}

function createWebsocketAndRun(products, channel) {
    var gdax_conn = createGDAXWebsocketClient(products);

   const websocketCallback = (data) => {
       if (data.type == 'ticker') {
           var msg = data.product_id + ' ' + data.price;
           channel.publish(data.product_id, '', new Buffer(data.price));
           console.log(msg);
       }
   }
   
   websocket.on('message', websocketCallback);
}

function createGDAXWebsocketClient(products) {
    const passPhrase = process.env.GDAX_CONNECTOR_PASSPHRASE
    const apiKey = process.env.GDAX_CONNECTOR_API_KEY;
    const base64secret = process.env.GDAX_CONNECTOR_BASE64_SECRET;
    const apiURI = process.env.GDAX_CONNECTOR_API_URL;

    const websocket = new GDAX.WebsocketClient(
        products,
        'wss://ws-feed.gdax.com',
        {
            key: apiKey,
            secret: base64secret,
            passphrase: passPhrase,
        },
        { channels: ['ticker'] }
    );

    return websocket;
}

function getProducts(callback) {
    const publicClient = new GDAX.PublicClient();
    
    publicClient.getProducts(function(error, response, data) {
        if (error) {
            callback(error, null);
            return;
        }

        var products = new Array();

        for (var i = 0; i < data.length; i++) {
            products.push(data[i].id);
        }

        callback(null, products);
    });
}
