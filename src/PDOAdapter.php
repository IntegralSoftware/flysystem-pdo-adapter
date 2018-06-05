<?php

namespace Integral\Flysystem\Adapter;

use League\Flysystem\Adapter\Polyfill\NotSupportingVisibilityTrait;
use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use League\Flysystem\Util;
use \PDO;

/**
 * Class PDOAdapter
 */
class PDOAdapter implements AdapterInterface
{
    use NotSupportingVisibilityTrait;

    /**
     * @var \PDO
     */
    protected $pdo;

    /**
     * @var string
     */
    protected $table;

    /**
     * PDOAdapter constructor.
     *
     * @param PDO    $pdo
     * @param string $tableName
     */
    public function __construct(PDO $pdo, $tableName)
    {
        $this->pdo = $pdo;

        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $tableName)) {
            throw new \InvalidArgumentException('Invalid table name');
        }

        $this->table = $tableName;
    }

    /**
     * {@inheritdoc}
     */
    public function write($path, $contents, Config $config)
    {
        $statement = $this->pdo->prepare("INSERT INTO {$this->table} (path, contents, size, type, mimetype, timestamp) VALUES(:path, :contents, :size, :type, :mimetype, :timestamp)");

        $size = strlen($contents);
        $type = 'file';
        $mimetype = Util::guessMimeType($path, $contents);
        $timestamp = $config->get('timestamp', time());

        $statement->bindParam(':path', $path, PDO::PARAM_STR);
        $statement->bindParam(':contents', $contents, PDO::PARAM_LOB);
        $statement->bindParam(':size', $size, PDO::PARAM_INT);
        $statement->bindParam(':type', $type, PDO::PARAM_STR);
        $statement->bindParam(':mimetype', $mimetype, PDO::PARAM_STR);
        $statement->bindParam(':timestamp', $timestamp, PDO::PARAM_INT);

        return $statement->execute() ? compact('path', 'contents', 'size', 'type', 'mimetype', 'timestamp') : false;
    }

    /**
     * {@inheritdoc}
     */
    public function writeStream($path, $resource, Config $config)
    {
        return $this->write($path, stream_get_contents($resource), $config);
    }

    /**
     * {@inheritdoc}
     */
    public function update($path, $contents, Config $config)
    {
        $statement = $this->pdo->prepare("UPDATE {$this->table} SET contents=:newcontents, mimetype=:mimetype, size=:size, timestamp=:timestamp WHERE path=:path");

        $size = strlen($contents);
        $mimetype = Util::guessMimeType($path, $contents);
        $timestamp = $config->get('timestamp', time());

        $statement->bindParam(':size', $size, PDO::PARAM_INT);
        $statement->bindParam(':mimetype', $mimetype, PDO::PARAM_STR);
        $statement->bindParam(':newcontents', $contents, PDO::PARAM_LOB);
        $statement->bindParam(':path', $path, PDO::PARAM_STR);
        $statement->bindParam(':timestamp', $timestamp, PDO::PARAM_INT);

        return $statement->execute() ? compact('path', 'contents', 'size', 'mimetype', 'timestamp') : false;
    }

    /**
     * {@inheritdoc}
     */
    public function updateStream($path, $resource, Config $config)
    {
        return $this->update($path, stream_get_contents($resource), $config);
    }

    /**
     * {@inheritdoc}
     */
    public function rename($path, $newpath)
    {
        $statement = $this->pdo->prepare("SELECT type FROM {$this->table} WHERE path=:path");
        $statement->bindParam(':path', $path, PDO::PARAM_STR);

        if ($statement->execute()) {
            $object = $statement->fetch(PDO::FETCH_ASSOC);

            if ($object['type'] === 'dir') {
                $dirContents = $this->listContents($path, true);

                $statement = $this->pdo->prepare("UPDATE {$this->table} SET path=:newpath WHERE path=:path");

                $pathLength = strlen($path);

                $statement->bindParam(':path', $currentObjectPath, PDO::PARAM_STR);
                $statement->bindParam(':newpath', $newObjectPath, PDO::PARAM_STR);

                foreach ($dirContents as $object) {
                    $currentObjectPath = $object['path'];
                    $newObjectPath = $newpath . substr($currentObjectPath, $pathLength);

                    $statement->execute();
                }
            }
        }

        $statement = $this->pdo->prepare("UPDATE {$this->table} SET path=:newpath WHERE path=:path");

        $statement->bindParam(':path', $path, PDO::PARAM_STR);
        $statement->bindParam(':newpath', $newpath, PDO::PARAM_STR);

        return $statement->execute();
    }

    /**
     * {@inheritdoc}
     */
    public function copy($path, $newpath)
    {
        $statement = $this->pdo->prepare("SELECT * FROM {$this->table} WHERE path=:path");

        $statement->bindParam(':path', $path, PDO::PARAM_STR);

        if ($statement->execute()) {
            $result = $statement->fetch(PDO::FETCH_ASSOC);

            if (!empty($result)) {
                $statement = $this->pdo->prepare("INSERT INTO {$this->table} (path, contents, size, type, mimetype, timestamp) VALUES(:path, :contents, :size, :type, :mimetype, :timestamp)");

                $statement->bindParam(':path', $newpath, PDO::PARAM_STR);
                $statement->bindParam(':contents', $result['contents'], PDO::PARAM_LOB);
                $statement->bindParam(':size', $result['size'], PDO::PARAM_INT);
                $statement->bindParam(':type', $result['type'], PDO::PARAM_STR);
                $statement->bindParam(':mimetype', $result['mimetype'], PDO::PARAM_STR);
                $statement->bindValue(':timestamp', time(), PDO::PARAM_INT);

                return $statement->execute();
            }
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function delete($path)
    {
        $statement = $this->pdo->prepare("DELETE FROM {$this->table} WHERE path=:path");

        $statement->bindParam(':path', $path, PDO::PARAM_STR);

        return $statement->execute();
    }

    /**
     * {@inheritdoc}
     */
    public function deleteDir($dirname)
    {
        $dirContents = $this->listContents($dirname, true);

        if (!empty($dirContents)) {
            $statement = $this->pdo->prepare("DELETE FROM {$this->table} WHERE path=:path");

            $statement->bindParam(':path', $currentObjectPath, PDO::PARAM_STR);

            foreach ($dirContents as $object) {
                $currentObjectPath = $object['path'];
                $statement->execute();
            }
        }

        $statement = $this->pdo->prepare("DELETE FROM {$this->table} WHERE path=:path AND type='dir'");

        $statement->bindParam(':path', $dirname, PDO::PARAM_STR);

        return $statement->execute();
    }

    /**
     * {@inheritdoc}
     */
    public function createDir($dirname, Config $config)
    {
        $statement = $this->pdo->prepare("INSERT INTO {$this->table} (path, type, timestamp) VALUES(:path, :type, :timestamp)");

        $timestamp = $config->get('timestamp', time());

        $statement->bindParam(':path', $dirname, PDO::PARAM_STR);
        $statement->bindValue(':type', 'dir', PDO::PARAM_STR);
        $statement->bindValue(':timestamp', $timestamp, PDO::PARAM_STR);

        return $statement->execute();
    }

    /**
     * {@inheritdoc}
     */
    public function has($path)
    {
        $statement = $this->pdo->prepare("SELECT id FROM {$this->table} WHERE path=:path");

        $statement->bindParam(':path', $path, PDO::PARAM_STR);

        if ($statement->execute()) {
            return (bool) $statement->fetch(PDO::FETCH_ASSOC);
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function read($path)
    {
        $statement = $this->pdo->prepare("SELECT contents FROM {$this->table} WHERE path=:path");

        $statement->bindParam(':path', $path, PDO::PARAM_STR);

        if ($statement->execute()) {
            return $statement->fetch(PDO::FETCH_ASSOC);
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function readStream($path)
    {
        $stream = fopen('php://temp', 'w+');
        $result = $this->read($path);

        if (!$result) {
            fclose($stream);

            return false;
        }

        fwrite($stream, $result['contents']);
        rewind($stream);

        return compact('stream');
    }

    /**
     * {@inheritdoc}
     */
    public function listContents($directory = '', $recursive = false)
    {
        $query = "SELECT path, size, type, mimetype, timestamp FROM {$this->table}";

        $useWhere = (bool) strlen($directory);

        if ($useWhere) {
            $query .= " WHERE path LIKE :path_prefix OR path=:path";
        }

        $statement = $this->pdo->prepare($query);

        if ($useWhere) {
            $pathPrefix = $directory . '/%';
            $statement->bindParam(':path_prefix', $pathPrefix, PDO::PARAM_STR);
            $statement->bindParam(':path', $directory, PDO::PARAM_STR);
        }

        if (!$statement->execute()) {
            return [];
        }

        $result = $statement->fetchAll(PDO::FETCH_ASSOC);

        $result = array_map(function($v) {
            $v['timestamp'] = (int) $v['timestamp'];
            $v['size'] = (int) $v['size'];
            $v['dirname'] = Util::dirname($v['path']);

            if ($v['type'] === 'dir') {
                unset($v['mimetype']);
                unset($v['size']);
                unset($v['contents']);
            }

            return $v;
        }, $result);

        return $recursive ? $result : Util::emulateDirectories($result);
    }

    /**
     * Get all the meta data of a file or directory.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getMetadata($path)
    {
        $statement = $this->pdo->prepare("SELECT id, path, size, type, mimetype, timestamp FROM {$this->table} WHERE path=:path");

        $statement->bindParam(':path', $path, PDO::PARAM_STR);

        if ($statement->execute()) {
            return $statement->fetch(PDO::FETCH_ASSOC);
        }

        return false;
    }

    /**
     * Get all the meta data of a file or directory.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getSize($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * Get the mimetype of a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getMimetype($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * Get the timestamp of a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getTimestamp($path)
    {
        return $this->getMetadata($path);
    }
}
