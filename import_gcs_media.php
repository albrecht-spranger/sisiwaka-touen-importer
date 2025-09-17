<?php

/**
 * GCS バケットを走査し、artwork_media に画像・動画の URL を投入します。
 * - 動画: kind='video', image_url=ポスター画像, video_url=動画
 * - 画像: kind='image', image_url=画像,      video_url=NULL
 *
 * 事前に composer で google/cloud-storage をインストールしてください:
 *   composer require google/cloud-storage:^1.40
 */

// ====== 関数 ======
function buildUrl(string $objectName): string
{
    $parts = explode('/', $objectName);
    $encoded = array_map('rawurlencode', $parts);
    return "https://storage.googleapis.com/sisiwaka-touen-medias/" . implode('/', $encoded);
}

// ====== メイン処理 ======
require __DIR__ . '/vendor/autoload.php';

use Google\Cloud\Storage\StorageClient;

// ======== 設定 =========
$GCS_BUCKET = 'sisiwaka-touen-medias';
$PROJECT_ID = 'test-pj-20250522';
$DB_USER = 'sisiwaka_editor';
$DB_PASS = getenv('DB_EDITOR_PW');
if ($DB_PASS === false) {
    throw new RuntimeException('DB_EDITOR_PW is not set in environment');
}

// MariaDB 接続情報（必要に応じて調整）
$DB_DSN = "mysql:host=localhost;dbname=sisiwaka_touen;charset=utf8mb4";

// 小ネタ：例外は即表示して終了（本番はロギング推奨）
set_exception_handler(function ($e) {
    fwrite(STDERR, "[ERROR] " . $e->getMessage() . PHP_EOL);
    exit(1);
});

// DSN 接続
$pdo = new PDO($DB_DSN, $DB_USER, $DB_PASS, [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
]);

// alt_ja/alt_en を必須とする前提の INSERT 文
$insertStmt = $pdo->prepare("
    INSERT INTO artwork_media
        (artwork_id, kind, image_url, video_url, sort_order, valid)
    VALUES
        (:artwork_id, :kind, :image_url, :video_url, :sort_order, 1)
");

// 削除ステートメント（対象作品IDの全行）
$deleteStmt = $pdo->prepare("DELETE FROM artwork_media WHERE artwork_id = :artwork_id");

// GCS クライアント
$storage = new StorageClient(); // 認証は GOOGLE_APPLICATION_CREDENTIALS に委ねる
$bucket  = $storage->bucket($GCS_BUCKET);

// ========== GCS を全走査して、作品IDごとにファイルを集約 ==========
$objects = $bucket->objects(['prefix' => '']);
$byArtwork = []; // [artwork_id(int) => files[]]
foreach ($objects as $obj) {
    $name = $obj->name();          // 例: "001/010_scene1.jpg"
    if (strpos($name, '/') === false) continue;

    [$folder, $filename] = explode('/', $name, 2);
    if (!preg_match('/^\d{3}$/', $folder)) continue;
    // フォルダ部分が3桁の数字でなければcontinue

    $dot = strrpos($filename, '.');
    if ($dot === false) continue;

    $base = substr($filename, 0, $dot);      // "010_scene1"
    $ext  = strtolower(substr($filename, $dot + 1)); // "jpg" / "mp4"
    if ($ext !== 'jpg' && $ext !== 'mp4') continue;

    $artworkId = (int)$folder;
    $byArtwork[$artworkId][] = [
        'name'     => $name,     // "001/010_scene1.jpg"
        'folder'   => $folder,   // "001"
        'filename' => $filename, // "010_scene1.jpg"
        'base'     => $base,     // "010_scene1"
        'ext'      => $ext,      // "jpg"
    ];
}

// ========== 作品IDごとに DELETE → INSERT（トランザクション単位） ==========
$totalDeleted = 0;
$totalInserted = 0;

foreach ($byArtwork as $artworkId => $files) {

    // base ごとに jpg/mp4 を突き合わせる
    $byBase = [];
    foreach ($files as $f) {
        $byBase[$f['base']][$f['ext']] = $f; // ['jpg'=>..., 'mp4'=>...]
    }

    // まず削除
    $pdo->beginTransaction();
    try {
        $deleteStmt->execute([':artwork_id' => $artworkId]);
        $deletedRows = $deleteStmt->rowCount();
        $totalDeleted += $deletedRows;
    } catch (Throwable $e) {
        $pdo->rollBack();
        throw $e;
    }

    // 次に挿入
    $insertCountForThisArtwork = 0;

    foreach ($byBase as $base => $parts) {
        // sort_order は base 先頭3桁の数字（なければ0）
        $sortOrder = (preg_match('/^(\d{3})/', $base, $m)) ? (int)$m[1] : 0;

        if (isset($parts['mp4'])) {
            if (!isset($parts['jpg'])) {
                fwrite(STDERR, "[WARN] poster jpg not found for base={$base} (artwork_id={$artworkId}), skip.\n");
                continue;
            }
            // 動画レコード: kind=video, image_url=poster(jpg or NULL), video_url=mp4
            $videoUrl = buildUrl($parts['mp4']['name']);
            $imageUrl = buildUrl($parts['jpg']['name']);

            try {
                $insertStmt->execute([
                    ':artwork_id' => $artworkId,
                    ':kind'       => 'video',
                    ':image_url'  => $imageUrl,
                    ':video_url'  => $videoUrl,
                    ':sort_order' => $sortOrder,
                ]);
                $insertCountForThisArtwork++;
            } catch (Throwable $e) {
                $pdo->rollBack();
                throw $e;
            }
        } elseif (isset($parts['jpg'])) {
            // 単体画像: kind=image, image_url=jpg, video_url=NULL
            $imgObj   = $parts['jpg'];
            $imageUrl = buildUrl($imgObj['name']);

            try {
                $insertStmt->execute([
                    ':artwork_id' => $artworkId,
                    ':kind'       => 'image',
                    ':image_url'  => $imageUrl,
                    ':video_url'  => null,
                    ':sort_order' => $sortOrder,
                ]);
                $insertCountForThisArtwork++;
            } catch (Throwable $e) {
                $pdo->rollBack();
                throw $e;
            }
        }
        // 他拡張子や特殊ケースは必要に応じて追加
    }

    // この作品IDの処理がすべて成功したらコミット
    $pdo->commit();
    $totalInserted += $insertCountForThisArtwork;
    echo "Synced artwork_id={$artworkId}: deleted, inserted {$insertCountForThisArtwork}\n";
}

echo "Done. Deleted rows (total, across artworks): {$totalDeleted}, Inserted rows: {$totalInserted}\n";
