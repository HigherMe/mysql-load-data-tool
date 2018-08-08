<?php

namespace ItsMill3rTime;

/**
 * Class MySQLFileTool
 * @package ItsMill3rTime
 */
class MySQLFileTool
{
    /**
     * @var array
     */
    protected $_files = [];
    /**
     * @var null
     */
    protected $_active_file = null;
    /**
     * @var int
     */
    protected $_current_index = 0;

    /**
     * @var null
     */
    protected $_active_file_reference = null;

    /**
     * @var
     */
    protected $_chunk_size;
    /**
     * @var
     */
    protected $_table;
    /**
     * @var
     */
    protected $_keys;

    /**
     * MySQLFileTool constructor.
     * @param $table
     * @param $keys
     * @param $chunk_size
     */
    public function __construct($table, $keys, $chunk_size)
    {
        $this->_table = $table;
        $this->_keys = $keys;
        $this->_chunk_size = $chunk_size;

    }

    /**
     * @return null|string
     */
    private function getActiveFile()
    {
        //if we need a new file
        if (is_null($this->_active_file)) {
            $this->_active_file = storage_path('SQL-' . uniqid());
            $this->_active_file_reference = fopen($this->_active_file, 'a');
            $this->_files[] = $this->_active_file;
        }

        //if we hit our chunk size
        if ($this->_current_index >= $this->_chunk_size) {
            fclose($this->_active_file_reference);
            //set active to null so we can make a new one
            $this->_active_file = null;
            $this->_current_index = 0;

            //create a new one
            return $this->getActiveFile();
        }

        return $this->_active_file;
    }

    /**
     *
     */
    public function import()
    {
        try {
            fclose($this->_active_file_reference);
        } catch (\Exception $e) {
        }
        foreach ($this->_files as $file) {
            try {
                \DB::connection()->getPdo()->exec("LOAD DATA LOCAL INFILE '" . $file . "' INTO TABLE " . $this->_table . " FIELDS TERMINATED BY '|||' (" . implode(",", $this->_keys) . ")");
                $this->destroyFile($file);
            } catch (\Exception $exception) {
                //keep failed file for review
                dump('file: ' . $file . ' failed' . $exception);
            }
        }
    }

    /**
     * @param array $row_fields_data
     */
    public function addRow(array $row_fields_data)
    {
        $row = implode('|||', $row_fields_data);
        fputs($this->_active_file_reference, $row . PHP_EOL);
        $this->_current_index++;
    }

    /**
     * @param $rows
     */
    public function addRows($rows)
    {
        $complete_string = '';
        foreach ($rows as $row_fields_data) {
            $row = implode('|||', $row_fields_data);
            $complete_string .= $row . PHP_EOL;
        }

        file_put_contents($this->getActiveFile(), $complete_string, FILE_APPEND | LOCK_EX);
        $this->_current_index = $this->_current_index + count($rows);
        $complete_string = null;
    }

    /**
     * @param $file
     */
    private function destroyFile($file)
    {
        @unlink($file);
    }
}