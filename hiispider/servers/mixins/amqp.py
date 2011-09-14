class AMQPMixin(object):

    amqp_setup = False

    def setupAMQP(self, config):
        if not self.amqp_setup:
            # Create AMQP Connection
            # AMQP connection parameters
            self.amqp_host = config["amqp_host"]
            self.amqp_port = config.get("amqp_port", 5672)
            self.amqp_username = config["amqp_username"]
            self.amqp_password = config["amqp_password"]
            self.amqp_queue = config["amqp_queue"]
            self.amqp_exchange = config["amqp_exchange"]
            self.amqp_prefetch_count = config.get("amqp_prefetch_count", 10)
            self.amqp_setup = True
