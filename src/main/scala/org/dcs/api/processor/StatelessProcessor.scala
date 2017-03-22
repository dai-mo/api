package org.dcs.api.processor

/**
  * Created by cmathew on 06/09/16.
  */
trait StatelessProcessor {

  var remoteProcessor: Option[RemoteProcessor] = None

  def instance(): RemoteProcessor = {
    if(remoteProcessor.isEmpty)
      remoteProcessor = Some(initialise())

    remoteProcessor.get
  }

  def initialise(): RemoteProcessor
}
