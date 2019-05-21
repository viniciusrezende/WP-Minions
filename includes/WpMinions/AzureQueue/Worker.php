<?php
namespace WpMinions\AzureQueue;
use WpMinions\Worker as BaseWorker;
use MicrosoftAzure\Storage\Queue\QueueRestProxy as QueueRestProxy;
use MicrosoftAzure\Storage\Queue\Models\ListMessagesOptions as ListMessagesOptions;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException as ServiceException;
use MicrosoftAzure\Storage\Queue\Models\CreateQueueOptions as CreateQueueOptions;

/**
 * Custom WP-Minions Worker class using AzureQueue
 */
class Worker extends BaseWorker {

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

    public function work() {
        $client = $this->get_azure_client();
        $message_options = new ListMessagesOptions();
        $message_options->setNumberOfMessages( 1 );
        $listMessagesResult = $client->listMessages( AZURESTORAGE_QUEUE, $message_options );
        $messages = $listMessagesResult->getQueueMessages();
        $result = false;
        $switched = false;
        try {
            if ( 0 < count( $messages ) && is_array( $messages ) ) {
                $message = $messages[0];
                $job_data = json_decode( $message->getMessageText(), true );
                $hook     = $job_data['hook'];
                $args     = $job_data['args'];
                if ( function_exists( 'is_multisite' ) && is_multisite() && $job_data['blog_id'] ) {
                    $blog_id = $job_data['blog_id'];
                    if ( get_current_blog_id() !== $blog_id ) {
                        switch_to_blog( $blog_id );
                        $switched = true;
                    } else {
                        $switched = false;
                    }
                } else {
                    $switched = false;
                }
                do_action( 'wp_async_task_before_job', $hook, $message );
                do_action( 'wp_async_task_before_job_' . $hook, $message );
                do_action( $hook, $args, $message );
                do_action( 'wp_async_task_after_job', $hook, $message );
                do_action( 'wp_async_task_after_job_' . $hook, $message );
                $messageId = $message->getMessageId();
                $popReceipt = $message->getPopReceipt();
                try    {
                    $client->deleteMessage( AZURESTORAGE_QUEUE, $messageId, $popReceipt );
                    $result = true;
                }
                catch( ServiceException $e ) {
                    $result = false;
                }
            }
        } catch ( ServiceException $e ) {
            $result = false;
        }
        if ( $switched ) {
            restore_current_blog();
        }
        return $result;
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