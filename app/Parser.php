<?php

namespace App;

use function chr;
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
use function pcntl_wait;
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
    private const W = 8;
    private const C = 262_144;

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

        $next = '';
        for ($i = 0; $i < 255; $i++) $next .= chr($i + 1);
        $next .= chr(255);

        $fh = fopen($in, 'rb');
        stream_set_read_buffer($fh, 0);
        $raw = fread($fh, 142_000);
        $rawLen = strlen($raw);

        $pl = []; $pi = []; $pc = 0; $pos = 0;
        while ($pc < 268 && $pos + 52 < $rawLen) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $s = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($pi[$s])) { $pi[$s] = $pc; $pl[$pc++] = $s; }
            $pos = $nl + 1;
        }
        unset($raw);

        $tailLength = 22;

        $maxStride = 0;
        $slugMap = [];
        for ($p = 0; $p < $pc; $p++) {
            $stride = strlen($pl[$p]) + 52;
            if ($stride > $maxStride) $maxStride = $stride;
            $slugMap[substr('https://stitcher.io/blog/' . $pl[$p], -$tailLength)] = ($stride << 20) | ($p * $dateCount);
        }
        $tailOffset = 26 + $tailLength;
        $outputSize = $pc * $dateCount;
        $fence = $maxStride * 16 + $tailOffset;

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
                $output = str_repeat("\0", $outputSize);
                $h = fopen($in, 'rb');
                stream_set_read_buffer($h, 0);
                fseek($h, $segments[$w][0]);
                $left = $segments[$w][1] - $segments[$w][0];
                $cs = self::C;

                while ($left > 0) {
                    $chunk = fread($h, $left > $cs ? $cs : $left);
                    if ($chunk === false || $chunk === '') break;
                    $cl = strlen($chunk); $left -= $cl;
                    $ln = strrpos($chunk, "\n");
                    if ($ln === false) break;
                    $t = $cl - $ln - 1;
                    if ($t > 0) { fseek($h, -$t, SEEK_CUR); $left += $t; }

                    $p = $ln;
                    while ($p > $fence) {
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                    }
                    while ($p >= $tailOffset) {
                        $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                        $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                        $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                    }
                }
                fclose($h);
                fwrite($pair[1], pack('v*', ...unpack('C*', $output)));
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $output = str_repeat("\0", $outputSize);
        stream_set_read_buffer($fh, 0);
        fseek($fh, $segments[self::W - 1][0]);
        $left = $segments[self::W - 1][1] - $segments[self::W - 1][0];
        $cs = self::C;

        while ($left > 0) {
            $chunk = fread($fh, $left > $cs ? $cs : $left);
            if ($chunk === false || $chunk === '') break;
            $cl = strlen($chunk); $left -= $cl;
            $ln = strrpos($chunk, "\n");
            if ($ln === false) break;
            $t = $cl - $ln - 1;
            if ($t > 0) { fseek($fh, -$t, SEEK_CUR); $left += $t; }

            $p = $ln;
            while ($p > $fence) {
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
            }
            while ($p >= $tailOffset) {
                $packed = $slugMap[substr($chunk, $p - $tailOffset, $tailLength)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20; $output[$idx] = $next[ord($output[$idx])];
            }
        }

        fclose($fh);
        $merged = pack('v*', ...unpack('C*', $output));
        unset($output);

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
        for ($w = 0; $w < self::W - 1; $w++) pcntl_wait($st);
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
}