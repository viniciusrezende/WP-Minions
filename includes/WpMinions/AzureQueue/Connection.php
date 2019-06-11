<?php

namespace WpMinions\AzureQueue;

use MicrosoftAzure\Storage\Queue\QueueRestProxy as QueueRestProxy;
use MicrosoftAzure\Storage\Queue\Models\CreateQueueOptions as CreateQueueOptions;
use MicrosoftAzure\Storage\Queue\Models\ListMessagesOptions as ListMessagesOptions;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException as ServiceException;
use MicrosoftAzure\Storage\Common\Internal\StorageServiceSettings as StorageServiceSettings;
use MicrosoftAzure\Storage\Common\Internal\Utilities as Utilities;
use MicrosoftAzure\Storage\Common\Internal\Authentication\SharedKeyAuthScheme as SharedKeyAuthScheme;
use MicrosoftAzure\Storage\Common\Internal\Middlewares\CommonRequestMiddleware as CommonRequestMiddleware;
use MicrosoftAzure\Storage\Queue\Internal\QueueResources as Resources;

/**
 * AzureQueue Connection class.
 */
class Connection {
	/**
	 * Connection.
	 *
	 * @var QueueRestProxy $connection AzureStorage Queue service client.
	 */
	private $connection;
	/**
	 * AzureQueue options.
	 *
	 * @var array $options Options.
	 */
	private $options;
	/**
	 * Connection String.
	 *
	 * @var string LOCALDEV_CONNECTION_STRING Storage emulator connection string.
	 */
	const LOCALDEV_CONNECTION_STRING = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;';

	/**
	 * Initialize the queue client from Azure Storage SDK and create the queue if it doesn't exist
	 */
	public function __construct() {
		global $azurequeue_options;

		if ( empty( $azurequeue_options ) ) {
			$azurequeue_options = array();
		}

		$azurequeue_options = wp_parse_args(
			$azurequeue_options,
			array(
				'connection_string'  => self::LOCALDEV_CONNECTION_STRING,
				'queue_name'         => 'wordpress',
				'queue_metadata'     => array(),
				'visibility_timeout' => null,
				'number_of_messages' => null,
				'gzipped'            => false,
			)
		);

		$this->options = $azurequeue_options;

		try {
			$settings      = StorageServiceSettings::createFromConnectionString(
				$azurequeue_options['connection_string']
			);
			$primary_uri   = Utilities::tryAddUrlScheme(
				$settings->getQueueEndpointUri()
			);
			$secondary_uri = Utilities::tryAddUrlScheme(
				$settings->getQueueSecondaryEndpointUri()
			);
			$queue_wrapper = new QueueRestProxy(
				$primary_uri,
				$secondary_uri,
				$settings->getName(),
				$this->options
			);

			// Getting authentication scheme.
			if ( $settings->hasSasToken() ) {
				$auth_scheme = new SharedAccessSignatureAuthScheme(
					$settings->getSasToken()
				);
			} else {
				$auth_scheme = new SharedKeyAuthScheme(
					$settings->getName(),
					$settings->getKey()
				);
			}
			// Adding common request middleware.
			$common_request_middleware = new CommonRequestMiddleware(
				$auth_scheme,
				Resources::STORAGE_API_LATEST_VERSION,
				Resources::QUEUE_SDK_VERSION
			);
			$queue_wrapper->pushMiddleware( $common_request_middleware );

			$this->connection = $queue_wrapper;
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
			$messages             = $list_messages_result->getQueueMessages();
			return $messages;
		} catch ( ServiceException $e ) {
			return false;
		}
	}

	/**
	 * Add message to queue
	 *
	 * @param string $message Message.
	 */
	public function add_message( $message ) {
		if ( empty( $this->connection ) ) {
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
	 *
	 * @param QueueMessage $message AzureQueue message object.
	 */
	public function delete_message( $message ) {
		if ( empty( $this->connection ) ) {
			return false;
		}

		$message_id  = $message->getMessageId();
		$pop_receipt = $message->getPopReceipt();

		try {
			$this->connection->deleteMessage( $this->get_queue(), $message_id, $pop_receipt );
			return true;
		} catch ( ServiceException $e ) {
			return false;
		}
	}

	/**
	 * Return is message is gzipped
	 *
	 * @return bool
	 */
	public function get_gzipped() {
			return $this->options['gzipped'];
	}

	/**
	 * Caches and returns the current blog id for adding to the Job meta
	 * data. False if not a multisite install.
	 *
	 * @return int|false The current blog ids id.
	 */
	function get_blog_id() {
		return function_exists( 'is_multisite' ) && is_multisite() ? get_current_blog_id() : false;
	}
}
