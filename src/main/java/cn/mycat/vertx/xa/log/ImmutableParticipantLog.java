
package cn.mycat.vertx.xa.log;

import cn.mycat.vertx.xa.State;

public class ImmutableParticipantLog {
    private final String target;
    private final long expires;
    private final State state;


    public ImmutableParticipantLog(String target,
                                   long expires,
                                   State txState) {
        this.target = target;
        this.expires = expires;
        this.state = txState;
    }

    public String getTarget() {
        return target;
    }

    public long getExpires() {
        return expires;
    }

    public State getState() {
        return state;
    }

    public ImmutableParticipantLog copy(State state){
        return new ImmutableParticipantLog(getTarget(),getExpires(),state);
    }

}
