<?php

namespace Integral\Flysystem\Adapter;

use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Adapter\Polyfill\NotSupportingVisibilityTrait;
use League\Flysystem\Config;
use League\Flysystem\Util;
use PDO;

/**
 * Class PDOAdapter
 */
class PDOAdapter extends AbstractAdapter
{
    use NotSupportingVisibilityTrait;

    /**
     * @var PDO
     */
    protected $pdo;

    /**
     * @var string
     */
    protected $table;

    /**
     * PDOAdapter constructor.
     *
     * @param PDO $pdo
     * @param string $tableName
     * @param string|null $pathPrefix
     */
    public function __construct(PDO $pdo, $tableName, $pathPrefix = null)
    {
        $this->pdo = $pdo;

        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $tableName)) {
            throw new \InvalidArgumentException('Invalid table name');
        }

        $this->setPathPrefix($pathPrefix);

        $this->table = $tableName;
    }

    /**
     * {@inheritdoc}
     */
    public function write($path, $contents, Config $config)
    {
        $statement = $this->pdo->prepare(
            "INSERT INTO {$this->table} (path, contents, size, type, mimetype, timestamp) VALUES(:path, :contents, :size, :type, :mimetype, :timestamp)"
        );

        $size = strlen($contents);
        $type = 'file';
        $mimetype = Util::guessMimeType($path, $contents);
        $timestamp = $config->get('timestamp', time());

        $pathWithPrefix = $this->applyPathPrefix($path);

        $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);
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
        $statement = $this->pdo->prepare(
            "INSERT INTO {$this->table} (path, contents, size, type, mimetype, timestamp) VALUES(:path, :contents, :size, :type, :mimetype, :timestamp)"
        );

        $size = 0; // see below
        $type = 'file';
        $mimetype = Util::guessMimeType($path, '');
        $timestamp = $config->get('timestamp', time());

        $pathWithPrefix = $this->applyPathPrefix($path);

        $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);
        $statement->bindParam(':contents', $resource, PDO::PARAM_LOB);
        $statement->bindParam(':size', $size, PDO::PARAM_INT);
        $statement->bindParam(':type', $type, PDO::PARAM_STR);
        $statement->bindParam(':mimetype', $mimetype, PDO::PARAM_STR);
        $statement->bindParam(':timestamp', $timestamp, PDO::PARAM_INT);
        $output = $statement->execute() ? compact('path', 'type', 'mimetype', 'timestamp') : false;

        if ($output) {
            // Correct the size afterwards
            // It seems all drivers are happy with LENGTH(binary)
            $statement = $this->pdo->prepare("UPDATE {$this->table} SET size = LENGTH(contents) WHERE path = :path");
            $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);
            $statement->execute();
        }

        return $output;
    }

    /**
     * {@inheritdoc}
     */
    public function update($path, $contents, Config $config)
    {
        $statement = $this->pdo->prepare(
            "UPDATE {$this->table} SET contents=:newcontents, mimetype=:mimetype, size=:size, timestamp=:timestamp WHERE path=:path"
        );

        $size = strlen($contents);
        $mimetype = Util::guessMimeType($path, $contents);
        $timestamp = $config->get('timestamp', time());

        $pathWithPrefix = $this->applyPathPrefix($path);

        $statement->bindParam(':size', $size, PDO::PARAM_INT);
        $statement->bindParam(':mimetype', $mimetype, PDO::PARAM_STR);
        $statement->bindParam(':newcontents', $contents, PDO::PARAM_LOB);
        $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);
        $statement->bindParam(':timestamp', $timestamp, PDO::PARAM_INT);

        return $statement->execute() ? compact('path', 'contents', 'size', 'mimetype', 'timestamp') : false;
    }

    /**
     * {@inheritdoc}
     */
    public function updateStream($path, $resource, Config $config)
    {
        $statement = $this->pdo->prepare(
            "UPDATE {$this->table} SET contents=:newcontents, mimetype=:mimetype, timestamp=:timestamp WHERE path=:path"
        );

        $mimetype = Util::guessMimeType($path, '');
        $timestamp = $config->get('timestamp', time());

        $pathWithPrefix = $this->applyPathPrefix($path);

        $statement->bindParam(':mimetype', $mimetype, PDO::PARAM_STR);
        $statement->bindParam(':newcontents', $resource, PDO::PARAM_LOB);
        $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);
        $statement->bindParam(':timestamp', $timestamp, PDO::PARAM_INT);
        $output = $statement->execute() ? compact('path', 'mimetype', 'timestamp') : false;

        if ($output) {
            // Correct the size afterwards
            // It seems all drivers are happy with LENGTH(binary)
            $statement = $this->pdo->prepare("UPDATE {$this->table} SET size = LENGTH(contents) WHERE path = :path");
            $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);
            $statement->execute();
        }

        return $output;
    }

    /**
     * {@inheritdoc}
     */
    public function rename($path, $newpath)
    {
        $pathWithPrefix = $this->applyPathPrefix($path);
        $statement = $this->pdo->prepare("SELECT type FROM {$this->table} WHERE path=:path");
        $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);

        if ($statement->execute()) {
            $object = $statement->fetch(PDO::FETCH_ASSOC);

            if ($object['type'] === 'dir') {
                $dirContents = $this->listContents($path, true);

                $statement = $this->pdo->prepare("UPDATE {$this->table} SET path=:newpath WHERE path=:path");

                $pathLength = strlen($path);

                $statement->bindParam(':path', $currentObjectPath, PDO::PARAM_STR);
                $statement->bindParam(':newpath', $newObjectPath, PDO::PARAM_STR);

                foreach ($dirContents as $object) {
                    $currentObjectPath = $this->applyPathPrefix($object['path']);
                    $newObjectPath = $this->applyPathPrefix($newpath.substr($object['path'], $pathLength));

                    $statement->execute();
                }
            }
        }

        $statement = $this->pdo->prepare("UPDATE {$this->table} SET path=:newpath WHERE path=:path");

        $pathWithPrefix = $this->applyPathPrefix($path);
        $newPathWithPrefix = $this->applyPathPrefix($newpath);
        $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);
        $statement->bindParam(':newpath', $newPathWithPrefix, PDO::PARAM_STR);

        return $statement->execute();
    }

    /**
     * {@inheritdoc}
     */
    public function copy($path, $newpath)
    {
        $newPathWithPrefix = $this->applyPathPrefix($newpath);
        $pathWithPrefix = $this->applyPathPrefix($path);
        // Use a one-liner to avoid race condition
        // between a $this->has() and the actual copy.
        return (bool)$this->pdo->exec(
            sprintf(
                "INSERT INTO %s (path, contents, size, type, mimetype, timestamp)
                          SELECT %s, contents, size, type, mimetype, timestamp FROM %s WHERE path = %s",
                $this->table,
                $this->pdo->quote($newPathWithPrefix),
                $this->table,
                $this->pdo->quote($pathWithPrefix)
            )
        );
    }

    /**
     * {@inheritdoc}
     */
    public function delete($path)
    {
        $statement = $this->pdo->prepare("DELETE FROM {$this->table} WHERE path=:path");
        $pathWithPrefix = $this->applyPathPrefix($path);
        $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);

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
                $currentObjectPath = $this->applyPathPrefix($object['path']);
                $statement->execute();
            }
        }

        $statement = $this->pdo->prepare("DELETE FROM {$this->table} WHERE path=:path AND type='dir'");
        $dirnameWithPrefix = $this->applyPathPrefix($dirname);
        $statement->bindParam(':path', $dirnameWithPrefix, PDO::PARAM_STR);

        return $statement->execute();
    }

    /**
     * {@inheritdoc}
     */
    public function createDir($dirname, Config $config)
    {
        $statement = $this->pdo->prepare(
            "INSERT INTO {$this->table} (path, type, timestamp) VALUES(:path, :type, :timestamp)"
        );

        $timestamp = $config->get('timestamp', time());

        $dirnameWithPrefix = $this->applyPathPrefix($dirname);

        $statement->bindParam(':path', $dirnameWithPrefix, PDO::PARAM_STR);
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

        $pathWithPrefix = $this->applyPathPrefix($path);
        $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);

        if ($statement->execute()) {
            return (bool)$statement->fetch(PDO::FETCH_ASSOC);
        }

        return false;
    }

    /**
     * This function prepare the way for read() and readStream()
     *
     * @param string $path
     *
     * @return \PDOStatement|false
     */
    public function readPrepare($path)
    {
        $statement = $this->pdo->prepare("SELECT contents FROM {$this->table} WHERE path=:path");

        $pathWithPrefix = $this->applyPathPrefix($path);
        $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);

        if ($statement->execute()) {
            return $statement;
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function read($path)
    {
        $statement = $this->readPrepare($path);
        if ($statement && ($result = $statement->fetch(PDO::FETCH_ASSOC))) {
            if (is_resource($result['contents'])) {
                // Some PDO drivers return a stream (as should be for LOB)
                // so we need to retrieve it entirely.
                $result['contents'] = stream_get_contents($result['contents']);
            }

            return $result;
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function readStream($path)
    {
        if (!($statement = $this->readPrepare($path))) {
            return false;
        }

        $statement->bindColumn(1, $stream, PDO::PARAM_LOB);
        if (!$statement->fetch(PDO::FETCH_BOUND)) {
            return false;
        }

        if (!is_resource($stream)) {
            // Some PDO drivers (MySQL, SQLite) don't return a stream, so we simulate one
            // see https://bugs.php.net/bug.php?id=40913
            $result = $stream;
            $stream = fopen('php://temp', 'w+');
            fwrite($stream, $result);
            rewind($stream);
        }

        return compact('stream');
    }

    /**
     * {@inheritdoc}
     */
    public function listContents($directory = '', $recursive = false)
    {
        $query = "SELECT path, size, type, mimetype, timestamp FROM {$this->table}";

        $useWhere = (bool)strlen($this->applyPathPrefix($directory));

        if ($useWhere) {
            $query .= " WHERE path LIKE :path_prefix OR path=:path";
        }

        $statement = $this->pdo->prepare($query);

        if ($useWhere) {
            $pathPrefix = $this->applyPathPrefix($directory.'/').'%';
            $statement->bindParam(':path_prefix', $pathPrefix, PDO::PARAM_STR);
            $directoryWithPrefix = $this->applyPathPrefix($directory);
            $statement->bindParam(':path', $directoryWithPrefix, PDO::PARAM_STR);
        }

        if (!$statement->execute()) {
            return [];
        }

        $result = $statement->fetchAll(PDO::FETCH_ASSOC);

        $result = array_map(
            function ($v) {
                $v['timestamp'] = (int)$v['timestamp'];
                $v['size'] = (int)$v['size'];
                $v['path'] = $this->removePathPrefix($v['path']);
                $v['dirname'] = Util::dirname($v['path']);

                if ($v['type'] === 'dir') {
                    unset($v['mimetype']);
                    unset($v['size']);
                    unset($v['contents']);
                }

                return $v;
            },
            $result
        );

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
        $statement = $this->pdo->prepare(
            "SELECT id, path, size, type, mimetype, timestamp FROM {$this->table} WHERE path=:path"
        );

        $pathWithPrefix = $this->applyPathPrefix($path);
        $statement->bindParam(':path', $pathWithPrefix, PDO::PARAM_STR);

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
