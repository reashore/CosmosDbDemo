function spHelloWorld() {
	let context = getContext();
	let response = context.getResponse();
	response.setBody('Greetings from the Cosmos DB server!');
}
