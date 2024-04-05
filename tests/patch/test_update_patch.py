from panda_patches.update_patch import UpdatePatch


def test_update_patch_eq():
    update1 = UpdatePatch(target={"id": 1, "id2": 4}, deltas={"value": 100})
    update2 = UpdatePatch(target={"id": 1, "id2": 4}, deltas={"value": 100})
    update3 = UpdatePatch(target={"id": 1, "id2": 4}, deltas={"value": 200})

    assert update1 == update2
    assert update1 != update3
