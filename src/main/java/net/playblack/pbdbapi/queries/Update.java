package net.playblack.pbdbapi.queries;

/**
 *
 * @author somners
 */
public class Update extends Where<Update> {

    @Override
    public Type getType() {
        return Type.UPDATE;
    }

}
