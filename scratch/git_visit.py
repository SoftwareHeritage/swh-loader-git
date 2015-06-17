# (* OCaml: non tail-recursive version *)

# let rec visit obj =
#   if not (is_in_cache obj) then begin
#     List.iter visit (get_parents obj);
#     store_object obj
#   end

# let _ = visit root_obj


# (* OCaml: (almost) tail-recursive version *)

# let visit obj =
#   let rec aux visit_todo store_todo =
#     match visit_todo with
#     | [] -> store_todo
#     | obj :: rest ->
#        if not (is_in_cache obj) then
# 	 let parents = get_parents obj in
#          (* "@" is not tail-rec in OCaml (length of 1st arg.), but we don't
#             care here, as equivalent operators might be tail-rec in other
#             languages. To be tail-rec in OCaml we would need List.rev_append
#             here, and to do reverse gymnastic elsewhere. *)
# 	 aux (rest @ parents) (obj :: store_todo)
#   in
#   let objects_to_store = aux [obj] [] in
#   List.iter store_object objects_to_store

# let _ = visit root_obj

from collections import deque

def visit(root_obj, cache, store):

    to_visit = deque([root_obj])  # FIFO
    to_store = deque()            # LIFO

    while to_visit:  # 1st pass: visit top-down, use cache, collect to_store
        obj = to_visit.popleft()  # extract from beginning (left)
        if obj not in cache:
            to_visit.extend(obj.parents)  # append to end (right)
            to_store.append(obj)

    while to_store:  # 2nd pass: store objects bottom-up
        obj = to_store.pop()
        store.add(obj)
