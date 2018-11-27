TEST_LOADER_CONFIG = {
    'storage': {
        'cls': 'memory',
        'args': {
        }
    },
    'send_contents': True,
    'send_directories': True,
    'send_revisions': True,
    'send_releases': True,
    'send_snapshot': True,

    'content_size_limit': 100 * 1024 * 1024,
    'content_packet_size': 10,
    'content_packet_size_bytes': 100 * 1024 * 1024,
    'directory_packet_size': 10,
    'revision_packet_size': 10,
    'release_packet_size': 10,

    'save_data': False,
}
