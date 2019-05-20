<?php
namespace WpMinions\AzureQueue;

use WpMinions\Worker as BaseWorker;

/**
 * Custom WP-Minions Worker class using AzureQueue
 */
class Worker extends BaseWorker {
    
    /**
	 * Instance of Connection class
	 *
	 * @var Connection
	 */
    public $connection = null;
    
    /**
	 * Setup backend
	 */
	public function register() {
		// Do nothing
    }
    
    /**
	 * Connect to Azure storage queue client
	 */
	private function connect() {
		if ( null !== $this->connection ) {
			return $this->connection;
		}

		try {
			$this->connection = new Connection();
		} catch ( \Exception $e ) {
			return false;
		}

		return $this->connection;
	}

    public function work() {
        if ( ! $this->connect() ) {
			return false;
        }
        
        $messages = $this->connection->get_messages();

        if ( false === $messages ) {
            return false;
        }

        $result = true;

        if ( 0 < count( $messages ) && is_array( $messages ) ) {
            foreach ( $messages as $message ) {
                $switched = false;

                $job_data = json_decode( $message->getMessageText(), true );
                $hook     = $job_data['hook'];
                $args     = $job_data['args'];
                
                if ( function_exists( 'is_multisite' ) && is_multisite() && false !== $job_data['blog_id'] ) {
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

                $result = $result && $this->connection->delete_message( $message );

                if ( $switched ) {
                    restore_current_blog();
                }
            }
        }

        return $result;
    }
}