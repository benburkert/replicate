require 'test/unit'
require 'stringio'
require 'replicate'

class LoaderTest < Test::Unit::TestCase
  def setup
    @loader = Replicate::Loader.new
  end

  def thing(attrs={})
    attrs = {'number' => 123, 'string' => 'hello', 'time' => Time.new}.merge(attrs)
    Replicate::Object.new attrs
  end

  def test_basic_filter
    called = false
    object = thing('test' => 'value')
    @loader.listen do |type, id, attrs, obj|
      assert !called
      assert_equal 'Replicate::Object', type
      assert_equal object.id, id
      assert_equal 'value', attrs['test']
      assert_equal object.attributes, attrs
      called = true
    end
    @loader.feed object.class, object.id, object.attributes
    assert called
  end

  def test_reading_from_io
    called = false
    data = Marshal.dump(['Replicate::Object', 10, {'test' => 'value'}])
    @loader.listen do |type, id, attrs, obj|
      assert !called
      assert_equal 'Replicate::Object', type
      assert_equal 'value', attrs['test']
      called = true
    end
    @loader.read(StringIO.new(data))
    assert called
  end

  def test_stats
    10.times do
      obj = thing
      @loader.feed obj.class, obj.id, obj.attributes
    end
    assert_equal({'Replicate::Object' => 10}, @loader.stats)
  end

  def test_block_form_runs_complete
    called = false
    Replicate::Loader.new do |loader|
      filter = lambda { |*args| }
      (class <<filter;self;end).send(:define_method, :complete) { called = true }
      loader.listen filter
      obj = thing
      loader.feed obj.class, obj.id, obj.attributes
      assert !called
    end
    assert called
  end

  def test_translating_id_attributes
    objects = []
    @loader.listen { |type, id, attrs, object| objects << object }

    object1 = thing
    @loader.feed object1.class, object1.id, object1.attributes
    object2 = thing('related' => [:id, 'Replicate::Object', object1.id])
    @loader.feed object2.class, object2.id, object2.attributes

    assert_equal 2, objects.size
    assert_equal objects[0].id, objects[1].related
  end

  def test_translating_multiple_id_attributes
    objects = []
    @loader.listen { |type, id, attrs, object| objects << object }

    members = (0..9).map { |i| thing('number' => i) }
    members.each do |member|
      @loader.feed member.class, member.id, member.attributes
    end

    ids = members.map { |m| m.id }
    referencer = thing('related' => [:id, 'Replicate::Object', ids])
    @loader.feed referencer.class, referencer.id, referencer.attributes

    assert_equal 11, objects.size
    assert_equal 10, objects.last.related.size
  end

  def test_ignoring_a_missing_type
    dumper = Replicate::Dumper.new

    objects = []
    dumper.listen { |type, id, attrs, obj| objects << [type, id, attrs, obj] }

    with_ghost_class do |klass|
      dumper.dump(klass.new)
    end

    begin
      objects.each { |type, id, attrs, obj| @loader.feed type, id, attrs }

      assert_fail "NameError unexpectedly ignored"
    rescue NameError
    end

    @loader.ignore_missing!

    objects.each { |type, id, attrs, obj| @loader.feed type, id, attrs }
  end

  def with_ghost_class
    eval <<-RUBY
      class ::Ghost
        def dump_replicant(dumper)
          dumper.write self.class, 3, {}, self
        end
      end
    RUBY

    yield Ghost
  ensure
    Object.send(:remove_const, :Ghost)
  end
end
