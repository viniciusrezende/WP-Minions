<?php

namespace WpMinions\AzureQueue;

use WpMinions\Client as BaseClient;

/**
 * Custom WP-Minions Client class using AzureQueue
 */
class Client extends BaseClient {

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
		// Do nothing.
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

	/**
	 * Caches and returns the current blog id for adding to the Job meta
	 * data. False if not a multisite install.
	 *
	 * @return int|false The current blog ids id.
	 */
	private function get_blog_id() {
		return function_exists( 'is_multisite' ) && is_multisite() ? get_current_blog_id() : false;
	}

	/**
	 * Adds a Job to the Client's Queue.
	 *
	 * @param string $hook The action hook name for the job.
	 * @param array  $args Optional arguments for the job.
	 * @return bool true or false depending on the Client.
	 */
	public function add( $hook, $args = array(), $priority ) {
		if ( ! $this->connect() ) {
			return false;
		}

		$job_data = array(
			'hook'    => $hook,
			'args'    => $args,
			'blog_id' => $this->connection->get_blog_id(),
		);

		return $this->connection->add_message( wp_json_encode( $job_data ) );
	}
}
