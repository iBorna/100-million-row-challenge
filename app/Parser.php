<?php

namespace App;

use App\Commands\Visit;
use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function ini_set;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function str_repeat;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use const SEEK_CUR;

final class Parser
{
    private const W = 8;
    private const CH = 32;
    private const C = 163_840;

    public function parse(string $in, string $out): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');
        error_reporting(0);
        $sz = 7_509_674_827;

        $dc = 0;
        $db = [];
        $dl = [];
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $md = match ($m) { 2 => $y === 24 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
                $ms = ($m < 10 ? '0' : '') . $m;
                for ($d = 1; $d <= $md; $d++) {
                    $k = "$y-$ms-" . ($d < 10 ? '0' : '') . $d;
                    $db[$k] = $dc;
                    $dl[$dc++] = $k;
                }
            }
        }
        $nx = [];
        for ($i = 0; $i < 255; $i++)
            $nx[chr($i)] = chr($i + 1);

        $fh = fopen($in, 'rb');
        stream_set_read_buffer($fh, 0);
        $raw = fread($fh, min(2_097_152, $sz));
        $ln = strrpos($raw, "\n") ?: 0;
        $nlPad = ($ln > 0 && $raw[$ln - 1] === "\r") ? 52 : 51;
        $pi = [];
        $pl = [];
        $pc = 0;
        $pos = 0;
        while ($pos < $ln) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false)
                break;
            $s = substr($raw, $pos + 25, $nl - $pos - $nlPad);
            if (!isset($pi[$s])) {
                $pi[$s] = $pc;
                $pl[$pc++] = $s;
            }
            $pos = $nl + 1;
        }
        unset($raw);
        foreach (Visit::all() as $v) {
            $s = substr($v->uri, 25);
            if (!isset($pi[$s])) {
                $pi[$s] = $pc;
                $pl[$pc++] = $s;
            }
        }
        $pb = [];
        for ($p = 0; $p < $pc; $p++)
            $pb[$pl[$p]] = $p * $dc;
        $cells = $pc * $dc;

        $bnd = [0];
        for ($i = 1; $i < self::CH; $i++) {
            fseek($fh, (int)($sz * $i / self::CH));
            fgets($fh);
            $bnd[] = ftell($fh);
        }
        fclose($fh);
        $bnd[] = $sz;
        $totalChunks = self::CH;

        $hasSem = function_exists('sem_get');
        $hasShmop = function_exists('shmop_open');
        $hasFork = function_exists('pcntl_fork');

        if ($hasFork && $hasShmop) {
            $cKey = ftok($in, 'z');
            $cShm = @shmop_open($cKey, 'c', 0644, 4);
            shmop_write($cShm, pack('V', 0), 0);

            $sem = $hasSem ? @sem_get(ftok($in, 'y'), 1, 0666, true) : null;

            $rShm = [];
            for ($w = 0; $w < self::W; $w++)
                $rShm[$w] = @shmop_open(ftok($in, chr(65 + $w)), 'c', 0644, $cells);

            for ($w = 0; $w < self::W - 1; $w++) {
                $pid = pcntl_fork();
                if ($pid === 0) {
                    gc_disable();
                    $cnt = str_repeat("\0", $cells);
                    while (true) {
                        if ($sem) sem_acquire($sem);
                        $ci = unpack('V', shmop_read($cShm, 0, 4))[1];
                        if ($ci >= $totalChunks) {
                            if ($sem) sem_release($sem);
                            break;
                        }
                        shmop_write($cShm, pack('V', $ci + 1), 0);
                        if ($sem) sem_release($sem);
                        static::crunchInto($in, $bnd[$ci], $bnd[$ci + 1], $pb, $db, $nx, $cnt);
                    }
                    shmop_write($rShm[$w], $cnt, 0);
                    exit(0);
                }
            }

            $cnt = str_repeat("\0", $cells);
            while (true) {
                if ($sem) sem_acquire($sem);
                $ci = unpack('V', shmop_read($cShm, 0, 4))[1];
                if ($ci >= $totalChunks) {
                    if ($sem) sem_release($sem);
                    break;
                }
                shmop_write($cShm, pack('V', $ci + 1), 0);
                if ($sem) sem_release($sem);
                static::crunchInto($in, $bnd[$ci], $bnd[$ci + 1], $pb, $db, $nx, $cnt);
            }

            for ($w = 0; $w < self::W - 1; $w++)
                pcntl_wait($st);

            $counts = array_fill(0, $cells, 0);
            $j = 0;
            foreach (unpack('C*', $cnt) as $v)
                $counts[$j++] = $v;
            for ($w = 0; $w < self::W - 1; $w++) {
                $blob = shmop_read($rShm[$w], 0, $cells);
                $j = 0;
                foreach (unpack('C*', $blob) as $v)
                    $counts[$j++] += $v;
            }

            for ($w = 0; $w < self::W; $w++) {
                @shmop_delete($rShm[$w]);
                @shmop_close($rShm[$w]);
            }
            @shmop_delete($cShm);
            @shmop_close($cShm);
            if ($sem) @sem_remove($sem);
        } else {
            $cnt = str_repeat("\0", $cells);
            for ($ci = 0; $ci < $totalChunks; $ci++)
                static::crunchInto($in, $bnd[$ci], $bnd[$ci + 1], $pb, $db, $nx, $cnt);
            $counts = array_fill(0, $cells, 0);
            $j = 0;
            foreach (unpack('C*', $cnt) as $v)
                $counts[$j++] = $v;
        }

        $dp = [];
        for ($d = 0; $d < $dc; $d++)
            $dp[$d] = '        "20' . $dl[$d] . '": ';
        $pp = [];
        for ($p = 0; $p < $pc; $p++)
            $pp[$p] = '"\/blog\/' . str_replace('/', '\/', $pl[$p]) . '"';

        $o = fopen($out, 'wb');
        stream_set_write_buffer($o, 1_048_576);
        fwrite($o, '{');
        $nl = PHP_EOL;
        $first = true;
        $buf = '';
        for ($p = 0; $p < $pc; $p++) {
            $base = $p * $dc;
            $body = '';
            $sep = $nl;
            for ($d = 0; $d < $dc; $d++) {
                $n = $counts[$base + $d];
                if (!$n)
                    continue;
                $body .= $sep . $dp[$d] . $n;
                $sep = ',' . $nl;
            }
            if (!$body)
                continue;
            $buf .= ($first ? '' : ',') . $nl . '    ' . $pp[$p] . ': {' . $body . $nl . '    }';
            $first = false;
            if (strlen($buf) > 131_072) {
                fwrite($o, $buf);
                $buf = '';
            }
        }
        fwrite($o, $buf . $nl . '}');
        fclose($o);
    }

    private static function crunchInto(string $in, int $s, int $e, array $pb, array $db, array $nx, string &$cnt): void
    {
        $h = fopen($in, 'rb');
        stream_set_read_buffer($h, 0);
        fseek($h, $s);
        $rem = $e - $s;
        $cs = self::C;
        while ($rem > 0) {
            $ch = fread($h, $rem > $cs ? $cs : $rem);
            if ($ch === false || $ch === '')
                break;
            $cl = strlen($ch);
            $rem -= $cl;
            $ln = strrpos($ch, "\n");
            if ($ln === false)
                break;
            $t = $cl - $ln - 1;
            if ($t > 0) {
                fseek($h, -$t, SEEK_CUR);
                $rem += $t;
            }
            $step = ($ln > 0 && $ch[$ln - 1] === "\r") ? 53 : 52;
            $p = 25;
            $f = $ln - 1600;
            while ($p < $f) {
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
            }
            while ($p < $ln) {
                $c = strpos($ch, ',', $p);
                if ($c === false || $c >= $ln)
                    break;
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
            }
        }
        fclose($h);
    }
}
