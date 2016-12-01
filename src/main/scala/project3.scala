import akka.actor.ActorSystem
import scala.collection.mutable.ListBuffer
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorSelection.toScala
import akka.actor.actorRef2Scala
import scala.util.control.Breaks._
import java.security.MessageDigest
import scala.util.Random
import akka.actor.ActorRef
import akka.dispatch.ExecutionContexts
import scala.util.Random
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer 
import scala.collection.mutable.HashMap
import scala.util.control.Breaks._
import scala.concurrent.Future
import akka.pattern.ask
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.math.pow

case class startnetwork()
case class addnode(nodestore:ActorRef,i:Integer,hashint2:Integer)
case class findpredsucc(selfid:ActorRef,i:Integer,hashint2:Integer)
case class findpredsucc2(identifier:Integer,hops:Integer)
case class predsuccfound(prede:ActorRef,predeint:Integer,succ:ActorRef,succint:Integer)
case class takepredsuccessor(predecessor:ActorRef,predeint:Integer,successor:ActorRef,succint:Integer)
case class addmenewpred(hashstring:Integer)
case class addedaspred(pred:ActorRef,predhash:Integer)
case class takesuccessorfinger(succ:ActorRef)
case class updatefingers(selfref:ActorRef,selfint:Integer)
case class addassucc(hashintpred:Integer)
case class succupdatefinger(succref:ActorRef,succint:Integer,i:Integer)
case class sendnewnodes(Id:Integer)
case class hopcount(hop:Integer)
case class startsearchingkeys(numreq:Integer)
case class takethecount(hopstotal:Integer)

 object main extends App{
     val master=ActorSystem("Chord").actorOf(Props(new Master(args(0).toInt,args(1).toInt )),name="Master")   
}
class Master(numnodes:Integer,numreq:Integer) extends Actor{
        var nodestore:Array[ActorRef]=new Array[ActorRef](numnodes)
        var hashtable = new HashMap[Int, Int]
        var m:Integer=28
        var range:Integer=1;
        for(i<-0 to m-1){
          range=range*2;
        }
        println("range is "+range)
        var totalmessages:Integer=numnodes*numreq
        //nodestore(0)=context.system.actorOf(Props(new Worker(self,0,numnodes,r,m,64,nodestore(0))),name="node"+0.toString())
       // println(r)
        for(i<-0 to numnodes-1) {
                        var randint=new scala.util.Random
                        var r:Integer=randint.nextInt(range-1)
                       println("for i value"+i)
                       hashtable(i)=r
                       println(r)
                       nodestore(i)=context.system.actorOf(Props(new Worker(totalmessages,self,i,numnodes,hashtable(i),m,range,nodestore(0))),name="node"+i.toString())
                       
      }           
            def receive={
                 case sendnewnodes(i:Integer)=>
                   println("the "+i+"th node has been added")
                   if(i<numnodes-1)
                   nodestore(0)! addnode(nodestore(i+1),i+1,hashtable(i+1))
                   
                 case takethecount(hopstotal:Integer)=>
                 //var hops:Integer=hopstotal
                   
                   println("key searching fnished")
                   var hopdouble:Double=hopstotal.toDouble
                   var messagedouble:Double=totalmessages.toDouble
                 var averagecount:Double=hopdouble/messagedouble
                 println("********************Total number of hops for number of nodes "+numnodes+" and message per node "+numreq+ " is************** "+hopstotal)
                 println("********************Average number of hops is************** "+averagecount)
                 System.exit(0)
            }
            
       nodestore(0)! startnetwork() 
       for(i<-1 to numnodes-1) {
         println(i+" and "+hashtable(i))
        nodestore(0)! addnode(nodestore(i),i,hashtable(i))
        
        Thread sleep 50
       }
        //nodestore(0)! addnode(nodestore(1),1,hashtable(1))
        
                                 
       
       
       Thread sleep 5000
       for(i<-0 to numnodes-1) {
        
        nodestore(i)! "print"
        //if(i==numnodes-1) 
        Thread sleep 10
       }
       Thread sleep 5000
       for(i<-0 to numnodes-1) {
         println("************************search key start**********************")
        nodestore(i)! startsearchingkeys(numreq)
        Thread sleep 100
       }
       
       
  // }
}
 class Worker(totalmessages:Integer,masternode:ActorRef,Id:Integer,numnodes:Integer,hashint:Integer,m:Integer,bigint:Integer,mainnode:ActorRef) extends Actor{
  var joined=false;
  var finger:Array[Integer]=new Array[Integer](m)
  var actorrefs:Array[ActorRef]=new Array[ActorRef](m)
  var pred:ActorRef=self
  var predhash:Integer=0
  var lastsenderpred:ActorRef=self
  var currnewnode:ActorRef=self
  var successorreturned:Integer=0;
  var hopstotal:Integer=0;
  var hopscount:Integer=0;
  var errormargin:Integer=40
  var consideredtotal:Integer=totalmessages
  def receive={
    
    
    ////// The assignment of a particular node as the main node which becomes the first node of the network.
    case startnetwork()=>
      
       if(finger(0)==null){
         for(i<-0 to m-1){
           finger(i)=hashint
           actorrefs(i)=self
         }
         predhash=hashint
         pred=self
      }
       
       
    ///// Printing the finger table   
    case "print"=>
      println("This is node "+Id+"my finger table is: ")
      for(i<-0 to m-1){
        println(finger(i))
      }
      
      
      
   ///// A main node receives the message from outside the nework which has the address of newnode 
   //// and this node adds the nodes in the network
    case addnode(newnode:ActorRef,newnodeid:Integer,hashint2:Integer)=>
               var nodeindex:Integer=0
               currnewnode=newnode
               successorreturned=0;
              self! findpredsucc(newnode,-1,hashint2)
              
              
              
 
    //////// Count the total number of hops taken by the nodes to find their message.          
    case hopcount(keycount:Integer)=>
         hopstotal=hopstotal+keycount;
         println("Total hops taken till now:"+hopstotal)
         hopscount=hopscount+1
       //  println("Hopscount value is "+hopscount)
         if(totalmessages>3000){
            consideredtotal=totalmessages-errormargin
         }
         if(hopscount>=(consideredtotal)){
           Thread sleep 1000
           masternode! takethecount(hopstotal)
         }
      
    /////// Finding the successor of nodes and the finger entries of the table. It will return 
    //////  the new added node the value of predecessor and successor if found or pass this message
    /////   to other actors
    case findpredsucc(newnode:ActorRef,j:Integer,identifier:Integer)=>
                           var check:Integer=0;
                           var i:Integer=m-1;
                           var index:Integer=0;
                           var nodemod:Integer=0;
                           var idmod:Integer=0;
                           var succmod:Integer=0;
                               if(finger(0)<hashint) {
                               succmod=finger(0)+bigint
                               }
                               else succmod=finger(0)
                               if(identifier<hashint){
                                             idmod=identifier+bigint;
                                     }
                               else if(identifier>(hashint+bigint)){
                                   idmod=identifier-bigint
                               }
                               else{
                                 idmod=identifier
                               }
                            if((finger(0)==hashint)){
                                if(j<0){
                                newnode! predsuccfound(self,hashint,actorrefs(0),finger(0))
                                }
                                else{
                               
                                  newnode! succupdatefinger(actorrefs(0),finger(0),j)
                                }
                            }
                            else if(idmod<succmod)
                            {
                             if(j<0){
                                newnode! predsuccfound(self,hashint,actorrefs(0),finger(0))
                                }
                                else{
                                  newnode! succupdatefinger(actorrefs(0),finger(0),j)
                                }

                            }
                               else{
                                  while(i>0){
                                     if(finger(i)<hashint){
                                            nodemod=finger(i)+bigint;
                                     }
                                     else nodemod=finger(i)
                                     println("")
                                     if(nodemod<idmod && check==0 && nodemod!=hashint){
                                              check=1;
                                              index=i;
                                     }
                                           i=i-1
                                     }
                                              println("value of index "+index)
                                              println("passing to other actors")
                                              actorrefs(index)! findpredsucc(newnode,j,identifier)
                                             // System.exit(0)
                                   
                                
                                 } 
                            
      /////// Finding the keys requested by the node.If a node finds the key then it will send
      /////   a message to the node which is calculating the average number of hops.
      //////  or it will pass the same message to other actor by increasing hop count by 1.
                 case findpredsucc2(identifier:Integer,hops:Integer)=>
                           var hopp:Integer=hops
                           var check:Integer=0;
                           var i:Integer=m-1;
                           var index:Integer=0;
                           var nodemod:Integer=0;
                           var idmod:Integer=0;
                           var succmod:Integer=0;
                               if(finger(0)<hashint) {
                               succmod=finger(0)+bigint
                               }
                               else succmod=finger(0)
                               if(identifier<hashint){
                                             idmod=identifier+bigint;
                                     }
                               else if(identifier>(hashint+bigint)){
                                   idmod=identifier-bigint
                               }
                               else{
                                 idmod=identifier
                               }
                           if((finger(0)==hashint)){
                                
                                  mainnode! hopcount(hops)
                                }
                            
                                 else if(idmod<succmod)
                            {
                             
                                  mainnode! hopcount(hops)
                                

                            }
                               else{
                                  while(i>0){
                                     if(finger(i)<hashint){
                                            nodemod=finger(i)+bigint;
                                     }
                                     else nodemod=finger(i)
                                     println("")
                                     if(nodemod<idmod && check==0 && nodemod!=hashint){
                                              check=1;
                                              index=i;
                                     }
                                           i=i-1
                                     }        
                                              hopp=hopp+1
                                              println("value of index "+index)
                                              println("passing to other actors")
                                              actorrefs(index)! findpredsucc2(identifier,hopp)
                                             // System.exit(0)
                                   
                                
                                 } 
                                
                            
                            
  /////////// Message that the network is built and nodes can start finding keys                          
  case startsearchingkeys(numreq:Integer)  =>
           var randint=new scala.util.Random
           for(i<-0 to numreq-1){
           Thread sleep 50  
           var key:Integer=randint.nextInt(bigint-1)
           self! findpredsucc2(key,1)
           }                  
                            
                            
                                
    ///// Predecessor and successor of the node found and value updated in the node.                        
    case predsuccfound(predecessor:ActorRef,predeint:Integer,successor:ActorRef,succint:Integer)=>
      println("succ and pred found at first " + succint+ " is new successor "+predeint+"is predecessor" )
      finger(0)=succint
      actorrefs(0)=successor
      predhash=predeint;
      pred=predecessor;
      successor! addmenewpred(hashint)
      
   ////////   successor updated in the finger table 
    case succupdatefinger(succ:ActorRef,succint:Integer,i:Integer)=>
      finger(i)=succint
      println("value of succ is: "+finger(i))
      actorrefs(i)=succ
    
      
    ////// Successor adding the new node as pred.  
     case addmenewpred(newpredhash)=>
       pred=sender
       predhash=newpredhash
       sender! "addedaspred"
       
     ///// message that successor has added it.
     case "addedaspred"=>
       pred! addassucc(hashint)
       
    //// Ask predecessor to add it in the network  
     case addassucc(predhashint)=>
       finger(0)=predhashint
       actorrefs(0)=sender
       sender! "addednewnodeassucc"
       
       
       
     //////////// When pred added as successor. The new node will start updating its finger table.We will follow the same logic
      ///////////// as mentioned in the pseudocode.
     case "addednewnodeassucc" =>
       println("Into finger table updation")
       var fingertemp:Integer=0
       for(i<-0 to m-2){
         finger(i+1)=hashint;
         actorrefs(i+1)=self;
       }
       
       for(i<-0 to m-2){
         if(finger(i)<hashint){
           
           fingertemp=finger(i)+bigint
         }
         else{
           fingertemp=finger(i)
           }
         var powervalue:Integer=pow(2,i+1).toInt
         if(fingertemp>(powervalue+hashint)){
           finger(i+1)=finger(i)
           actorrefs(i+1)=actorrefs(i)
           
         }
         else{
           mainnode! findpredsucc(self,i+1,hashint+powervalue)
         }
       }
       
       //////// 
       pred! updatefingers(self,hashint)
       
       case "relevantnodesupdated"=>
         println("predecessor updated relevant nodes")
         
         
         
     case updatefingers(newnode,newnodehashint)=>
       var allupdated:Integer=0
       println("entering into update others ")
       var newnodetemp:Integer=0;
       var fingertemp:Integer=0;
       var i=m-1;
       var lastentrynode:Integer=0
       ////Condition for comparing. A little peculiar
       if(newnodehashint<hashint) newnodetemp=newnodehashint+bigint
       else newnodetemp=newnodehashint;
       
       while(i>=0){
         if(finger(i)<hashint) fingertemp=finger(i)+bigint
         else fingertemp=finger(i)
         var powervalue:Integer=pow(2,i).toInt
        if(newnodetemp>(hashint+powervalue) && ( (newnodetemp<fingertemp)||(fingertemp==hashint))){
          allupdated=1
           finger(i)=newnodehashint
           actorrefs(i)=newnode
         }
         i=i-1;
       }
       if(allupdated==1)
       pred!updatefingers(newnode,newnodehashint)
       else
         newnode! "relevantnodesupdated"
       
       
  
  }
  }
