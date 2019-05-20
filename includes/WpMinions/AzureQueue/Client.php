<?php

namespace WpMinions\AzureQueue;

use WpMinions\Client as BaseClient;
use MicrosoftAzure\Storage\Queue\QueueRestProxy as QueueRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException as ServiceException;
use MicrosoftAzure\Storage\Queue\Models\CreateQueueOptions as CreateQueueOptions;

/**
 * Custom WP-Minions Client class using AzureQueue
 */
class Client extends BaseClient {

    private $azure_client = null;

    public function register() {
        $client = $this->get_azure_client();

        if ( $client !== false ) {
            $client->createQueue( AZURESTORAGE_QUEUE );
            return true;
        } else {
            return false;
        }
    }

    public function add( $hook, $args = array(), $priority = 'normal' ) {
        $job_data = array(
            'hook'    => $hook,
            'args'    => $args,
            'blog_id' => $this->get_blog_id(),
        );

        $client = $this->get_azure_client();

        if ( $client !== false ) {
            $payload  = json_encode( $job_data );
            try {
                $client->createMessage( AZURESTORAGE_QUEUE, $payload );
                return true;
            } catch ( ServiceException $e ) {
                return false;
            }
        } else {
            return false;
        }
    }

    /* Helpers */
    /**
     * Returns Azure Client
     */
    function get_azure_client() {
        if ( is_null( $this->azure_client ) ) {
            $this->azure_client = QueueRestProxy::createQueueService( AZURESTORAGE_CONNECTION );
        }

        return $this->azure_client;
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