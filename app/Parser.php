<?php

namespace App;

use function chr;
use function chunk_split;
use function fclose;
use function feof;
use function fgets;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function intdiv;
use function pcntl_fork;
use function sodium_add;
use function str_repeat;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use const SEEK_CUR;
use const SEEK_END;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    private const W     = 8;
    private const C     = 131_072;
    private const SHIFT = 20;

    public function parse(string $in, string $out): void
    {
        gc_disable();

        $dateIds = [];
        $datePrefixes = [];
        $dateCount = 0;
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) { 2 => $y === 4 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
                $ms = ($m < 10 ? '0' : '') . $m;
                $ym = "{$y}-{$ms}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ym . ($d < 10 ? '0' : '') . $d;
                    $dateIds[$key] = $dateCount;
                    $datePrefixes[$dateCount] = '        "202' . $key . '": ';
                    $dateCount++;
                }
            }
        }

        $next = [];
        for ($i = 0; $i < 255; $i++) $next[chr($i)] = chr($i + 1);

        $fh = fopen($in, 'rb');
        stream_set_read_buffer($fh, 0);
        $raw = fread($fh, 142_000);

        $pl = []; $pi = []; $pc = 0; $pos = 0;
        $rawLen = strlen($raw);
        while ($pc < 268 && $pos + 52 < $rawLen) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $s = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($pi[$s])) { $pi[$s] = $pc; $pl[$pc++] = $s; }
            $pos = $nl + 1;
        }
        unset($raw);

        $mask = (1 << self::SHIFT) - 1;
        $maxStride = 0;
        $slugMap = [];
        for ($p = 0; $p < $pc; $p++) {
            $stride = strlen($pl[$p]) + 52;
            if ($stride > $maxStride) $maxStride = $stride;
            $slugMap[substr('https://stitcher.io/blog/' . $pl[$p], -22)] = ($stride << self::SHIFT) | ($p * $dateCount);
        }
        $outputSize = $pc * $dateCount;
        $fence = $maxStride * 12 + 48;

        fseek($fh, 0, SEEK_END);
        $fileSize = ftell($fh);

        $segments = [];
        for ($w = 0; $w < self::W; $w++) {
            $from = intdiv($fileSize * $w, self::W);
            $to   = intdiv($fileSize * ($w + 1), self::W);
            if ($from > 0) { fseek($fh, $from); fgets($fh); $from = ftell($fh); }
            if ($w < self::W - 1) { fseek($fh, $to); fgets($fh); $to = ftell($fh); }
            else { $to = $fileSize; }
            $segments[$w] = [$from, $to];
        }

        $sockets = [];
        for ($w = 0; $w < self::W - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize * 2);
            stream_set_chunk_size($pair[1], $outputSize * 2);
            if (pcntl_fork() === 0) {
                gc_disable();
                fwrite($pair[1], chunk_split(static::crunchSegment(
                    $in, $segments[$w][0], $segments[$w][1],
                    $slugMap, $dateIds, $outputSize, $next, $fence, $mask
                ), 1, "\0"));
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $merged = chunk_split(static::crunchSegment(
            $in, $segments[self::W - 1][0], $segments[self::W - 1][1],
            $slugMap, $dateIds, $outputSize, $next, $fence, $mask
        ), 1, "\0");
        fclose($fh);

        $buffers = [];
        $write = []; $except = [];
        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, null);
            foreach ($read as $key => $sock) {
                $data = fread($sock, $outputSize * 2);
                if ($data !== '' && $data !== false) $buffers[$key] = ($buffers[$key] ?? '') . $data;
                if (feof($sock)) { fclose($sock); unset($sockets[$key]); }
            }
        }

        for ($w = 0; $w < self::W - 1; $w++) sodium_add($merged, $buffers[$w]);
        unset($buffers);

        $counts = unpack('v*', $merged);
        unset($merged);

        $escapedPaths = [];
        for ($p = 0; $p < $pc; $p++)
            $escapedPaths[$p] = '"\/blog\/' . $pl[$p] . '": {';

        $o = fopen($out, 'wb');
        stream_set_write_buffer($o, 2_097_152);
        fwrite($o, '{');

        $sep = "\n    ";
        $base = 1;
        for ($p = 0; $p < $pc; $p++) {
            $start = $base;
            $limit = $base + $dateCount;
            while ($base < $limit && $counts[$base] === 0) $base++;
            if ($base === $limit) continue;
            $buf = $sep . $escapedPaths[$p] . "\n" . $datePrefixes[$base - $start] . $counts[$base];
            $sep = ",\n    ";
            for ($base++; $base < $limit; $base++) {
                if ($counts[$base] !== 0)
                    $buf .= ",\n" . $datePrefixes[$base - $start] . $counts[$base];
            }
            fwrite($o, $buf . "\n    }");
        }

        fwrite($o, "\n}");
        fclose($o);
    }

    private static function crunchSegment(
        string $in, int $from, int $to,
        array $slugMap, array $dateIds, int $outputSize, array $next,
        int $fence, int $mask
    ): string {
        $output = str_repeat("\0", $outputSize);
        $h = fopen($in, 'rb');
        stream_set_read_buffer($h, 0);
        fseek($h, $from);
        $left = $to - $from;
        $cs   = self::C;
        $sh   = self::SHIFT;

        while ($left > 0) {
            $chunk = fread($h, $left > $cs ? $cs : $left);
            if ($chunk === false || $chunk === '') break;
            $cl    = strlen($chunk);
            $left -= $cl;
            $ln    = strrpos($chunk, "\n");
            if ($ln === false) break;
            $tail = $cl - $ln - 1;
            if ($tail > 0) { fseek($h, -$tail, SEEK_CUR); $left += $tail; }

            $p = $ln;

            while ($p > $fence) {
                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;

                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;
            }

            while ($p >= 48) {
                $packed = $slugMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - 22, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p -= $packed >> $sh;
            }
        }

        fclose($h);
        return $output;
    }
}