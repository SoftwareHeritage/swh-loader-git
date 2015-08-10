import pickle
import zlib
import os

#file_path_sample = '/home/tony/work/inria/repo/swh-git-loader-testdata/.git/objects/08/2a4aac696b8645e39771c2ba61e6e4a4aaf989'
file_path_sample = '/home/tony/work/inria/repo/swh-git-loader-testdata/.git/objects/26/4f1dacc1460bddb84c3e3e2eeb8b6f03fd172e'

def read_git_object_content(filepath):
    if os.path.exists(filepath):
        with open(filepath, 'rb') as f:
            content = f.read()

        return zlib.decompress(content)
    return None

map_content = { 'lion': 'yellow'
              , 'kitty': 'red'
              , 'content': read_git_object_content(file_path_sample)
              }

# marshall/serialize/pickle
pickle.dump(map_content, open( 'save.p', 'wb' ))

# unmarshal/deserialize/unpickle
map_content_loaded = pickle.load( open( 'save.p', 'rb' ) )
# {'content': b'tree 37\x00100644 README.md\x00\x8b\xbb\x0b\xcc6\x9a\xb3\xba\xa4#\xda\xcc6\x00a\x81:1\x05\xf3', 'lion': 'yellow', 'kitty': 'red'}
