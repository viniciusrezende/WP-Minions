<?php

namespace WpMinions\AzureQueue;

use MicrosoftAzure\Storage\Queue\QueueRestProxy as QueueRestProxy;
use MicrosoftAzure\Storage\Queue\Models\CreateQueueOptions as CreateQueueOptions;
use MicrosoftAzure\Storage\Queue\Models\ListMessagesOptions as ListMessagesOptions;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException as ServiceException;

class Connection {

    private $connection;
    private $options;
    
    private $localdev_connection_string = 'localhostDefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;';

	/**
	 * Initialize the queue client from Azure Storage SDK and create the queue if it doesn't exist
	 */
	public function __construct() {
		global $azurequeue_options;

        if ( empty( $azurequeue_options ) ) {
            $azurequeue_options = array();
        }

        $azurequeue_options = wp_parse_args( $azurequeue_options, array(
            'connection_string'  => $localdev_connection_string,
            'queue_name'         => 'wordpress',
            'queue_metadata'     => array(),
            'visibility_timeout' => null,
            'number_of_messages' => null,
        ) );

        $this->options = $azurequeue_options;

        try {
            $this->connection = new QueueRestProxy( $azurequeue_options['connection_string'] );
        } catch ( ServiceException $e ) {
            throw new \Exception( 'Could not create connection.' );
        }

        $create_queue_options = new CreateQueueOptions();

        if ( ! empty( $azurequeue_options['queue_metadata'] ) ) {
            foreach ( $azurequeue_options['queue_metadata'] as $queuemeta_key => $queuemeta_value ) {
                $create_queue_options->addMetaData( $queuemeta_key, $queuemeta_value );
            }
        }

        try {
            $this->connection->createQueue( $this->get_queue(), $create_queue_options );
        } catch ( ServiceException $e ) {
            throw new \Exception( 'Could not create queue.' );
        }

        add_action( 'shutdown', array( $this, 'shutdown' ) );
	}

	/**
	 * Return the queue name
	 *
	 * @return string
	 */
	public function get_queue() {
		return $this->options['queue_name'];
    }
    
    /**
	 * Get messages from queue
	 */
	public function get_messages() {
		if ( empty( $this->connection ) ) {
			return false;
		}

        $message_options = new ListMessagesOptions();
        
        if ( array_key_exists( 'visibility_timeout', $this->options ) && ! empty( $this->options['visibility_timeout'] ) ) {
            $message_options->setVisibilityTimeoutInSeconds( $this->options['visibility_timeout'] );    
        }

        if ( array_key_exists( 'number_of_messages', $this->options ) && ! empty( $this->options['number_of_messages'] ) ) {
            $message_options->setNumberOfMessages( $this->options['number_of_messages'] );    
        }

        try {
            $list_messages_result = $this->connection->listMessages( $this->get_queue(), $message_options );
            $messages = $list_messages_result->getQueueMessages();
            return $messages;
        } catch ( ServiceException $e ) {
            return false;
        }
    }
    
    /**
	 * Add message to queue
	 */
	public function add_message( $message ) {
		if ( empty( $this->connection ) ) ) {
			return false;
		}

        try {
            $this->connection->createMessage( $this->get_queue(), $message );
            return true;
        } catch ( ServiceException $e ) {
            return false;
        }
    }
    
    /**
	 * Delete message from queue
	 */
	public function delete_message( $message ) {
		if ( empty( $this->connection ) ) ) {
			return false;
		}

        $message_id = $message->getMessageId();
        $pop_receipt = $message->getPopReceipt();

        try {
            $this->connection->deleteMessage( $this->get_queue(), $message_id, $pop_receipt );
            return true;
        } catch ( ServiceException $e ) {
            return false;
        }
	}
}
